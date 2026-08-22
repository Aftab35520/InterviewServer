# IMPORTANT: gevent monkey_patch must be the very first import.
# This makes all stdlib I/O (sockets, threads, sleep) cooperative
# so 1000+ green-threads can share a single OS thread without blocking.
from gevent import monkey
monkey.patch_all()
from flask_cors import CORS
from flask import Flask, jsonify, request, make_response
from flask_socketio import SocketIO, emit
import gevent
import gevent.queue

from collections import deque
import threading
import os
import sys
from pymongo import MongoClient
from dotenv import load_dotenv
import razorpay
import time
import hmac
import hashlib

load_dotenv()
RAZORPAY_KEY_ID = os.getenv("RAZORPAY_KEY_ID")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET")
RAZORPAY_WEBHOOK_SECRET = os.getenv("RAZORPAY_WEBHOOK_SECRET")

razorpay_client = razorpay.Client(
    auth=(
        RAZORPAY_KEY_ID,
        RAZORPAY_KEY_SECRET
    )
)
from agent.agent_call import agent_call

# ── MongoDB (reuse auth module's connection) ─────────────────────────────────
# Add parent dir so we can import auth.py from the root
# Read MongoDB configuration from environment variables
MONGO_URI = os.getenv("MONGODB_URI")  # or "MONGO_URI"
DB_NAME   = os.getenv("MONGODB_DB_NAME", "interviewhelper")  # default fallback
COLL      = os.getenv("MONGODB_COLLECTION", "users")  # default fallback

try:
    _mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000,tlsAllowInvalidCertificates=True )
    _db = _mongo_client[DB_NAME]
    MONGO_AVAILABLE = True
    print(f"[MongoDB] Connected successfully to database: {DB_NAME}")
except Exception as _e:
    print(f"[MongoDB] Connection failed: {_e}")
    MONGO_AVAILABLE = False
    _db = None
    _mongo_client = None

app = Flask(__name__)
CORS(app)
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "interview-helper-secret")

# ── Rate limiter ─────────────────────────────────────────────────────────────
# Sliding-window: 15 requests per 60 seconds, per connected client (sid).
RATE_LIMIT        = 15      # max requests
RATE_WINDOW_SEC   = 60      # rolling window in seconds
_rate_lock        = threading.Lock()
_rate_buckets: dict[str, deque] = {}  # sid -> deque of request timestamps


def _check_rate_limit(sid: str) -> bool:
    """Returns True if the request is allowed, False if rate-limited."""
    now = time.monotonic()
    cutoff = now - RATE_WINDOW_SEC
    with _rate_lock:
        bucket = _rate_buckets.setdefault(sid, deque())
        # Evict timestamps outside the rolling window
        while bucket and bucket[0] <= cutoff:
            bucket.popleft()
        if len(bucket) >= RATE_LIMIT:
            return False
        bucket.append(now)
        return True

# gevent async_mode: handles 1000+ concurrent WebSocket connections.
# max_http_buffer_size: allow large payloads (e.g. base64 screenshots ~500KB)
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="gevent",
    ping_timeout=60,
    ping_interval=25,
    max_http_buffer_size=10 * 1024 * 1024,  # 10 MB — handles full screenshots
)


# ── REST endpoints ───────────────────────────────────────────────────────────

@app.route("/")
def home():
    return jsonify({"status": "ok", "message": "Interview Helper server running"})


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


# ── Credit Purchase Page ──────────────────────────────────────────────────────

def _get_credit_seconds(google_id: str) -> int:
    """Read current credit from MongoDB."""
    if not MONGO_AVAILABLE or _db is None or not google_id:
        return 0
    try:
        user = _db[COLL].find_one({"google_id": google_id}, {"credit_seconds": 1})
        return int((user or {}).get("credit_seconds", 0))
    except Exception:
        return 0


def _add_credit_seconds(google_id: str, amount: int) -> int:
    """Atomically increment credit and return the new total."""
    if not MONGO_AVAILABLE or _db is None or not google_id:
        return 0
    try:
        result = _db[COLL].find_one_and_update(
            {"google_id": google_id},
            {"$inc": {"credit_seconds": amount}},
            return_document=True,
            upsert=False,
        )
        return int((result or {}).get("credit_seconds", 0))
    except Exception as e:
        print(f"[Buy] add_credit_seconds error: {e}")
        return 0

import time
from flask import jsonify


@app.route("/create-order/<user_id>", methods=["GET"])
def Create_razorpay_order(user_id):

    # -----------------------------------------
    # Validate user ID
    # -----------------------------------------

    if not user_id:
        return jsonify({
            "success": False,
            "message": "user_id is required"
        }), 400

    # -----------------------------------------
    # Check database
    # -----------------------------------------

    if not MONGO_AVAILABLE or _db is None:
        return jsonify({
            "success": False,
            "message": "Database unavailable"
        }), 503

    # -----------------------------------------
    # Find user
    # -----------------------------------------

    user = _db[COLL].find_one({
        "google_id": str(user_id)
    })

    if not user:
        return jsonify({
            "success": False,
            "message": "User not found"
        }), 404

    # -----------------------------------------
    # Payment information
    # -----------------------------------------

    amount_paise = 99 * 100
    credit_seconds = 3600

    receipt = f"credit_{user_id}_{int(time.time())}"

    # -----------------------------------------
    # Create Razorpay order
    # -----------------------------------------

    try:

        order = razorpay_client.order.create({

            "amount": amount_paise,

            "currency": "INR",

            "receipt": receipt,

            "notes": {
                "google_id": str(user_id),
                "product": "interview_ai_1_hour",
                "credit_seconds": str(credit_seconds)
            }
        })

    except Exception as e:

        print(
            "[Razorpay] Order creation failed:",
            repr(e)
        )

        return jsonify({
            "success": False,
            "message": "Could not create Razorpay order"
        }), 500

    # -----------------------------------------
    # SAVE PAYMENT IN SEPARATE COLLECTION
    # -----------------------------------------

    try:

        payment = {

            # User reference
            "google_id": str(user_id),

            # Razorpay
            "razorpay_order_id": order["id"],
            "razorpay_payment_id": None,
            "razorpay_signature": None,

            # Payment
            "amount": amount_paise,
            "currency": "INR",

            # Product
            "product": "interview_ai_1_hour",
            "credit_seconds": credit_seconds,

            # Status
            "status": "created",

            # Receipt
            "receipt": receipt,

            # Timestamps
            "created_at": time.time(),
            "paid_at": None
        }

        _db["payments"].insert_one(payment)

    except Exception as e:

        print(
            "[MongoDB] Payment save failed:",
            repr(e)
        )

        return jsonify({
            "success": False,
            "message": "Could not save payment"
        }), 500

    # -----------------------------------------
    # Return order to React
    # -----------------------------------------

    return jsonify({

        "success": True,

        "order_id": order["id"],

        "amount": amount_paise,

        "currency": "INR",

        "key_id": RAZORPAY_KEY_ID
    })






@app.route("/verify-payment", methods=["POST"])
def Verify_razorpay_payment():

    # =========================================
    # Get payment data from React
    # =========================================

    data = request.get_json()

    if not data:
        return jsonify({
            "success": False,
            "message": "Request body is required"
        }), 400

    razorpay_payment_id = data.get(
        "razorpay_payment_id"
    )

    razorpay_order_id = data.get(
        "razorpay_order_id"
    )

    razorpay_signature = data.get(
        "razorpay_signature"
    )

    # =========================================
    # Validate data
    # =========================================

    if not razorpay_payment_id:
        return jsonify({
            "success": False,
            "message": "razorpay_payment_id is required"
        }), 400

    if not razorpay_order_id:
        return jsonify({
            "success": False,
            "message": "razorpay_order_id is required"
        }), 400

    if not razorpay_signature:
        return jsonify({
            "success": False,
            "message": "razorpay_signature is required"
        }), 400

    # =========================================
    # Database check
    # =========================================

    if not MONGO_AVAILABLE or _db is None:
        return jsonify({
            "success": False,
            "message": "Database unavailable"
        }), 503

    # =========================================
    # Find our payment record
    # =========================================

    payment = _db["payments"].find_one({
        "razorpay_order_id": razorpay_order_id
    })

    if not payment:

        return jsonify({
            "success": False,
            "message": "Payment order not found"
        }), 404

    # =========================================
    # Prevent duplicate processing
    # =========================================

    if payment.get("status") == "paid":

        return jsonify({
            "success": True,
            "message": "Payment already verified",
            "credit_added": False
        }), 200

    # =========================================
    # Generate signature
    # =========================================

    message = (
        f"{razorpay_order_id}|"
        f"{razorpay_payment_id}"
    )

    generated_signature = hmac.new(
        RAZORPAY_KEY_SECRET.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    # =========================================
    # Compare signatures
    # =========================================

    if not hmac.compare_digest(
        generated_signature,
        razorpay_signature
    ):

        print(
            "[Razorpay] Invalid payment signature"
        )

        return jsonify({
            "success": False,
            "message": "Invalid payment signature"
        }), 400

    # =========================================
    # Get user from OUR payment record
    # =========================================

    google_id = payment.get("google_id")

    if not google_id:

        return jsonify({
            "success": False,
            "message": "User reference missing from payment"
        }), 500

    # =========================================
    # Find user
    # =========================================

    user = _db[COLL].find_one({
        "google_id": str(google_id)
    })

    if not user:

        return jsonify({
            "success": False,
            "message": "User not found"
        }), 404

    # =========================================
    # Update payment
    # =========================================

    payment_update = _db["payments"].update_one(

        {
            "_id": payment["_id"],
            "status": "created"
        },

        {
            "$set": {

                "razorpay_payment_id":
                    razorpay_payment_id,

                "razorpay_signature":
                    razorpay_signature,

                "status":
                    "paid",

                "paid_at":
                    time.time()
            }
        }
    )

    # =========================================
    # Make sure payment was updated
    # =========================================

    if payment_update.modified_count != 1:

        # Another request may have processed it
        latest_payment = _db["payments"].find_one({
            "_id": payment["_id"]
        })

        if latest_payment and \
           latest_payment.get("status") == "paid":

            return jsonify({
                "success": True,
                "message": "Payment already processed",
                "credit_added": False
            }), 200

        return jsonify({
            "success": False,
            "message": "Could not update payment"
        }), 500

    # =========================================
    # ADD INTERVIEW CREDIT
    # =========================================

    credit_seconds = payment.get(
        "credit_seconds",
        3600
    )

    _db[COLL].update_one(

        {
            "google_id": str(google_id)
        },

        {
            "$inc": {
                "credit_seconds": credit_seconds
            }
        }
    )

    # =========================================
    # Success
    # =========================================

    return jsonify({

        "success": True,

        "message":
            "Payment verified and credit added",

        "google_id":
            str(google_id),

        "credit_added":
            credit_seconds

    }), 200








@app.route("/add-credit", methods=["POST"])
def add_credit():
    """Add credit_seconds to a user's account.
    Body: { "uid": "<google_id>", "seconds": <int> }
    """
    data    = request.get_json(force=True) or {}
    uid     = data.get("uid", "").strip()
    seconds = int(data.get("seconds", 600))

    if not uid:
        return jsonify({"ok": False, "error": "Missing uid"}), 400
    if seconds <= 0 or seconds > 7200:
        return jsonify({"ok": False, "error": "Invalid seconds value"}), 400

    new_total = _add_credit_seconds(uid, seconds)
    if new_total == 0 and not MONGO_AVAILABLE:
        return jsonify({"ok": False, "error": "Database not available"}), 503

    print(f"[Buy] uid={uid[:8]}… +{seconds}s  total={new_total}s")
    return jsonify({"ok": True, "credit_seconds": new_total})




@app.route("/razorpay/webhook", methods=["POST"])
def razorpay_webhook():

    # =========================================
    # Get Razorpay signature
    # =========================================

    webhook_signature = request.headers.get(
        "X-Razorpay-Signature"
    )

    if not webhook_signature:

        return jsonify({
            "success": False,
            "message": "Missing webhook signature"
        }), 400

    # =========================================
    # IMPORTANT:
    # Use RAW request body for verification
    # =========================================

    raw_body = request.get_data()

    # =========================================
    # Generate signature
    # =========================================

    generated_signature = hmac.new(
        RAZORPAY_WEBHOOK_SECRET.encode("utf-8"),
        raw_body,
        hashlib.sha256
    ).hexdigest()

    # =========================================
    # Verify webhook authenticity
    # =========================================

    if not hmac.compare_digest(
        generated_signature,
        webhook_signature
    ):

        print(
            "[Razorpay Webhook] Invalid signature"
        )

        return jsonify({
            "success": False,
            "message": "Invalid webhook signature"
        }), 400

    # =========================================
    # Parse JSON AFTER signature verification
    # =========================================

    payload = request.get_json()

    event = payload.get("event")

    print(
        "[Razorpay Webhook] Event:",
        event
    )

    # =========================================
    # Handle payment.captured
    # =========================================

    if event == "payment.captured":

        try:

            payment_entity = (
                payload
                .get("payload", {})
                .get("payment", {})
                .get("entity", {})
            )

            razorpay_payment_id = payment_entity.get(
                "id"
            )

            razorpay_order_id = payment_entity.get(
                "order_id"
            )

            amount = payment_entity.get(
                "amount"
            )

            status = payment_entity.get(
                "status"
            )

            # -----------------------------------------
            # Validate required Razorpay data
            # -----------------------------------------

            if not razorpay_payment_id:
                return jsonify({
                    "success": False,
                    "message": "Payment ID missing"
                }), 400

            if not razorpay_order_id:
                return jsonify({
                    "success": False,
                    "message": "Order ID missing"
                }), 400

            # -----------------------------------------
            # Find our payment
            # -----------------------------------------

            payment = _db["payments"].find_one({
                "razorpay_order_id":
                    razorpay_order_id
            })

            if not payment:

                print(
                    "[Webhook] Payment order not found:",
                    razorpay_order_id
                )

                # Return 200 so Razorpay doesn't
                # repeatedly retry an unknown order.
                return jsonify({
                    "success": True,
                    "message": "Order not found locally"
                }), 200

            # -----------------------------------------
            # Already processed?
            # -----------------------------------------

            if payment.get("status") == "paid":

                print(
                    "[Webhook] Payment already processed:",
                    razorpay_order_id
                )

                return jsonify({
                    "success": True,
                    "message": "Already processed"
                }), 200

            # -----------------------------------------
            # Verify amount
            # -----------------------------------------

            if amount != payment.get("amount"):

                print(
                    "[Webhook] Amount mismatch:",
                    razorpay_order_id
                )

                return jsonify({
                    "success": False,
                    "message": "Amount mismatch"
                }), 400

            # -----------------------------------------
            # Verify payment status
            # -----------------------------------------

            if status != "captured":

                print(
                    "[Webhook] Payment not captured:",
                    status
                )

                return jsonify({
                    "success": True,
                    "message": "Payment not captured"
                }), 200

            # -----------------------------------------
            # Get user
            # -----------------------------------------

            google_id = payment.get(
                "google_id"
            )

            if not google_id:

                print(
                    "[Webhook] google_id missing:",
                    razorpay_order_id
                )

                return jsonify({
                    "success": False,
                    "message": "User reference missing"
                }), 500

            # -----------------------------------------
            # Mark payment as paid
            #
            # IMPORTANT:
            # Only update if current status is
            # still "created".
            #
            # This prevents duplicate credit.
            # -----------------------------------------

            payment_update = _db["payments"].update_one(

                {
                    "_id": payment["_id"],
                    "status": "created"
                },

                {
                    "$set": {

                        "razorpay_payment_id":
                            razorpay_payment_id,

                        "status":
                            "paid",

                        "paid_at":
                            time.time(),

                        "webhook_event":
                            event
                    }
                }
            )

            # -----------------------------------------
            # Was this request the one that
            # successfully changed the payment?
            # -----------------------------------------

            if payment_update.modified_count != 1:

                print(
                    "[Webhook] Payment already processed:"
                    ,
                    razorpay_order_id
                )

                return jsonify({
                    "success": True,
                    "message": "Already processed"
                }), 200

            # -----------------------------------------
            # Add InterviewAI credit
            # -----------------------------------------

            credit_seconds = payment.get(
                "credit_seconds",
                3600
            )

            _db[COLL].update_one(

                {
                    "google_id":
                        str(google_id)
                },

                {
                    "$inc": {
                        "interview_credits":
                            credit_seconds
                    }
                }
            )

            print(
                "[Webhook] Credit added:",
                google_id,
                credit_seconds
            )

        except Exception as e:

            print(
                "[Razorpay Webhook] Processing error:",
                repr(e)
            )

            return jsonify({
                "success": False,
                "message": "Webhook processing failed"
            }), 500

    # =========================================
    # Payment failed
    # =========================================

    elif event == "payment.failed":

        try:

            payment_entity = (
                payload
                .get("payload", {})
                .get("payment", {})
                .get("entity", {})
            )

            razorpay_payment_id = payment_entity.get(
                "id"
            )

            razorpay_order_id = payment_entity.get(
                "order_id"
            )

            if razorpay_order_id:

                _db["payments"].update_one(

                    {
                        "razorpay_order_id":
                            razorpay_order_id,

                        "status": {
                            "$ne": "paid"
                        }
                    },

                    {
                        "$set": {

                            "razorpay_payment_id":
                                razorpay_payment_id,

                            "status":
                                "failed",

                            "failed_at":
                                time.time()
                        }
                    }
                )

        except Exception as e:

            print(
                "[Webhook] Failed-payment error:",
                repr(e)
            )

            return jsonify({
                "success": False
            }), 500

    # =========================================
    # Other events
    # =========================================

    else:

        print(
            "[Razorpay Webhook] Ignored event:",
            event
        )

    # =========================================
    # IMPORTANT:
    # Return 200 to Razorpay
    # =========================================

    return jsonify({
        "success": True
    }), 200



# ── Socket.IO events ─────────────────────────────────────────────────────────

@socketio.on("connect")
def on_connect():
    sid = request.sid
    print(f"[+] Client connected  sid={sid}")
    emit("connected", {
        "message": "Connected to Interview Helper backend",
        "sid": sid,
    })


@socketio.on("disconnect")
def on_disconnect():
    sid = request.sid
    print(f"[-] Client disconnected  sid={sid}")
    # Clean up rate-limiter state for this client
    with _rate_lock:
        _rate_buckets.pop(sid, None)


@socketio.on("query")
def on_query(data):
    """
    Accepts:
      { "query": "<text>" }                         — plain text query
      { "query": "<text>", "image": "<base64 png>" } — screen analysis (multimodal)

    thread_id = socket session id → each client has isolated LangGraph memory.

    Streams:  response_chunk  -> { "chunk": "<text>" }
    Emits:    response_done   -> { "message": "stream complete" }
    Emits:    response_error  -> { "error": "<message>" }
    """
    sid        = request.sid
    data       = data or {}
    query_text = data.get("query", "").strip()
    image_b64  = data.get("image", "")  # optional base64 PNG
    if not query_text:
        emit("response_error", {"error": "No query provided"})
        return

    if not _check_rate_limit(sid):
        emit("response_error", {
            "error": f"Rate limit exceeded — max {RATE_LIMIT} requests per {RATE_WINDOW_SEC}s. Please wait before sending another query."
        })
        return

    print(f"[Q] sid={sid[:8]}…  {'[IMG] ' if image_b64 else ''}{query_text[:80]}")
    # Build HumanMessage — multimodal if an image was included
    if image_b64:
        message_content = [
            {"type": "text", "text": query_text},
            # JPEG MIME type — desktop sends JPEG-compressed screenshots (~80-120KB)
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_b64}"}},
        ]
    else:
        message_content = query_text
    # Bridge: LLM runs in a real OS thread (httpx needs it); socketio.emit
    # must run on the gevent event loop. Use a stdlib queue.Queue (truly
    # thread-safe) + polling consumer that yields with gevent.sleep(0).
    import queue as _stdlib_queue
    q = _stdlib_queue.Queue()

    def _producer():
        """Runs in a real OS thread — safe for httpx/SSL."""
        try:
            for chunk in agent_call(message_content, thread_id=sid):
                q.put(("chunk", chunk))
            q.put(("done", None))
        except Exception as exc:
            print(f"[!] sid={sid[:8]}… error: {exc}")
            q.put(("error", str(exc)))

    def _consumer():
        """Runs as a gevent greenlet — safe for socketio.emit.
        Polls the stdlib Queue with a short timeout and yields between polls."""
        while True:
            try:
                kind, value = q.get(timeout=0.05)
            except _stdlib_queue.Empty:
                gevent.sleep(0)   # yield to event loop, then retry
                continue
            if kind == "chunk":
                socketio.emit("response_chunk", {"chunk": value}, to=sid)
                gevent.sleep(0)   # yield so gevent can flush to the polling client
            elif kind == "done":
                socketio.emit("response_done", {"message": "stream complete"}, to=sid)
                break
            elif kind == "error":
                socketio.emit("response_error", {"error": value}, to=sid)
                break

    threading.Thread(target=_producer, daemon=True).start()
    gevent.spawn(_consumer)


# ── Entry point ──────────────────────────────────────────────────────────────
#
# ── Development ─────────────────────────────────────────────────────────────
#   python main.py
#
# ── Production (100+ concurrent users) ──────────────────────────────────────
#   gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker \
#            -w 1 --worker-connections 1000 \
#            -b 0.0.0.0:5001 main:app
#
if __name__ == "__main__":
    socketio.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", 5001)),
        debug=False,
        use_reloader=False,
        allow_unsafe_werkzeug=True,
    )