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

# ── MongoDB ───────────────────────────────────────────────────────────────────
MONGO_URI = os.getenv("MONGODB_URI")
DB_NAME   = os.getenv("MONGODB_DB_NAME", "interviewhelper")
COLL      = os.getenv("MONGODB_COLLECTION", "users")
COUPONS_COLL = os.getenv("MONGODB_COUPONS_COLL", "coupons")

_mongo_client = None
_db = None

def _get_db():
    global _mongo_client, _db
    if _db is not None:
        return _db
    if not MONGO_URI:
        return None
    try:
        _mongo_client = MongoClient(
            MONGO_URI,
            connect=False,
            serverSelectionTimeoutMS=5000,
            tlsAllowInvalidCertificates=True,
        )
        _db = _mongo_client[DB_NAME]
        return _db
    except Exception as _e:
        print(f"[MongoDB] Connection failed: {_e}")
        return None

def is_mongo_available():
    return _get_db() is not None

app = Flask(__name__)
CORS(app)
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "interview-helper-secret")

# ── App Version Management ───────────────────────────────────────────────────
MIN_APP_VERSION    = os.getenv("MIN_APP_VERSION", "1.0.0")
LATEST_APP_VERSION = os.getenv("LATEST_APP_VERSION", "1.0.0")
APP_DOWNLOAD_URL   = os.getenv("APP_DOWNLOAD_URL", "https://www.aftaro.com/download")
INITIAL_FREE_CREDIT_SECONDS = int(os.getenv("INITIAL_FREE_CREDIT_SECONDS", "1200"))  # 20 minutes for new users

# ── Rate limiter ─────────────────────────────────────────────────────────────
# Sliding-window: 10 requests per 60 seconds (configurable), per connected client (sid).
RATE_LIMIT        = int(os.getenv("RATE_LIMIT", "10"))  # max requests per window
RATE_WINDOW_SEC   = int(os.getenv("RATE_WINDOW_SEC", "60"))  # rolling window in seconds
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

def _parse_version(v_str: str) -> tuple:
    """Safely parse version string like '1.0.0' into numeric tuple (1, 0, 0)."""
    try:
        parts = [int(p) for p in v_str.strip().split(".") if p.isdigit()]
        return tuple(parts) if parts else (0, 0, 0)
    except Exception:
        return (0, 0, 0)


@app.route("/")
def home():
    return jsonify({
        "status": "ok",
        "app": "Aftaro Backend",
        "min_version": MIN_APP_VERSION,
        "latest_version": LATEST_APP_VERSION,
    })


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/api/version", methods=["GET"])
def check_version():
    """
    Client passes ?version=1.0.0.
    Returns whether update is required (force_update) and latest version info.
    """
    client_version_str = request.args.get("version", "").strip()
    force_update = False
    if client_version_str:
        client_ver = _parse_version(client_version_str)
        min_ver = _parse_version(MIN_APP_VERSION)
        if client_ver < min_ver:
            force_update = True

    return jsonify({
        "success": True,
        "min_version": MIN_APP_VERSION,
        "latest_version": LATEST_APP_VERSION,
        "download_url": APP_DOWNLOAD_URL,
        "force_update": force_update,
        "message": "Update required to continue using Aftaro." if force_update else "App version is up to date.",
    })


# ── Google OAuth & Auth Endpoints (Secured on Backend) ────────────────────────
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
_GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
_GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"
COUPONS_COLL = os.getenv("MONGODB_COUPONS_COLL", "coupons")


@app.route("/api/auth/google", methods=["POST"])
def auth_google():
    """
    Exchange authorization code for tokens, fetch Google user info,
    upsert user in MongoDB with device_token, and return user document.
    """
    data = request.get_json(silent=True) or {}
    code = data.get("code")
    redirect_uri = data.get("redirect_uri", "http://localhost:8765")
    device_token = data.get("device_token", "")

    if not code:
        return jsonify({"success": False, "message": "Missing authorization code"}), 400

    try:
        import requests as _req
        token_resp = _req.post(_GOOGLE_TOKEN_URL, data={
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "code": code,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }, timeout=15)
        token_data = token_resp.json()

        if "error" in token_data:
            return jsonify({
                "success": False,
                "message": f"Token exchange failed: {token_data.get('error_description', token_data['error'])}"
            }), 400

        access_token = token_data.get("access_token")
        if not access_token:
            return jsonify({"success": False, "message": "No access token in response"}), 400

        userinfo_resp = _req.get(
            _GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
        google_info = userinfo_resp.json()
        if "error" in google_info or "id" not in google_info:
            return jsonify({"success": False, "message": "Failed to fetch user profile from Google"}), 400

        google_id = google_info["id"]
        now = time.time()
        
        user_doc = {
            "google_id": google_id,
            "email": google_info.get("email", ""),
            "name": google_info.get("name", ""),
            "picture": google_info.get("picture", ""),
            "current_device_token": device_token,
            "profile": {},
            "credit_seconds": INITIAL_FREE_CREDIT_SECONDS,
        }

        db = _get_db()
        if db is not None:
            update_doc = {
                "$set": {
                    "email": google_info.get("email", ""),
                    "name": google_info.get("name", ""),
                    "picture": google_info.get("picture", ""),
                    "last_login": now,
                    "current_device_token": device_token,
                },
                "$setOnInsert": {
                    "google_id": google_id,
                    "profile": {},
                    "credit_seconds": INITIAL_FREE_CREDIT_SECONDS,
                    "created_at": now,
                }
            }
            db[COLL].update_one({"google_id": google_id}, update_doc, upsert=True)
            db_user = db[COLL].find_one({"google_id": google_id}, {"_id": 0})
            if db_user:
                user_doc = db_user

        return jsonify({"success": True, "user": user_doc})

    except Exception as e:
        print(f"[Auth] Google auth error: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/api/auth/validate-session", methods=["POST"])
def validate_session_endpoint():
    """
    Validate local session token against MongoDB to enforce single-device login.
    """
    data = request.get_json(silent=True) or {}
    google_id = data.get("google_id")
    device_token = data.get("device_token", "")

    if not google_id:
        return jsonify({"status": "not_found", "message": "Missing google_id"}), 400

    db = _get_db()
    if db is None:
        return jsonify({"status": "db_error", "message": "Database unavailable"})

    try:
        user = db[COLL].find_one({"google_id": google_id}, {"_id": 0})
        if not user:
            return jsonify({"status": "not_found", "message": "User not found"})

        db_token = user.get("current_device_token", "")
        if device_token and db_token and db_token != device_token:
            return jsonify({"status": "kicked", "message": "Logged in on another device"})

        return jsonify({"status": "valid", "user": user})
    except Exception as e:
        print(f"[Auth] validate_session error: {e}")
        return jsonify({"status": "db_error", "message": str(e)})


@app.route("/api/user/profile/<google_id>", methods=["GET"])
def get_user_profile(google_id):
    """Retrieve user profile & credit from MongoDB."""
    db = _get_db()
    if db is None:
        return jsonify({"success": False, "message": "DB not available"}), 503
    try:
        user = db[COLL].find_one({"google_id": google_id}, {"_id": 0})
        if user:
            return jsonify({
                "success": True,
                "profile": user.get("profile", {}),
                "credit_seconds": int(user.get("credit_seconds", 0))
            })
        return jsonify({"success": False, "message": "User not found"}), 404
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/api/user/profile", methods=["POST"])
def save_user_profile():
    """Save user profile to MongoDB."""
    data = request.get_json(silent=True) or {}
    google_id = data.get("google_id")
    profile = data.get("profile", {})

    if not google_id:
        return jsonify({"success": False, "message": "Missing google_id"}), 400

    db = _get_db()
    if db is None:
        return jsonify({"success": False, "message": "DB not available"}), 503

    try:
        db[COLL].update_one({"google_id": google_id}, {"$set": {"profile": profile}}, upsert=True)
        return jsonify({"success": True, "message": "Profile saved"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/api/user/credits/<google_id>", methods=["GET"])
def get_credits_endpoint(google_id):
    """Get remaining credit seconds for a user."""
    seconds = _get_credit_seconds(google_id)
    return jsonify({"success": True, "credit_seconds": seconds})


@app.route("/api/user/credits/sync", methods=["POST"])
def sync_credits_endpoint():
    """Persist credit seconds to MongoDB."""
    data = request.get_json(silent=True) or {}
    google_id = data.get("google_id")
    seconds = data.get("credit_seconds")

    if not google_id or seconds is None:
        return jsonify({"success": False, "message": "Missing google_id or credit_seconds"}), 400

    db = _get_db()
    if db is None:
        return jsonify({"success": False, "message": "DB not available"}), 503

    try:
        db[COLL].update_one({"google_id": google_id}, {"$set": {"credit_seconds": max(0, int(seconds))}}, upsert=True)
        return jsonify({"success": True, "credit_seconds": seconds})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/api/user/redeem-coupon", methods=["POST"])
def redeem_coupon_endpoint():
    """Atomic single-use coupon redemption."""
    data = request.get_json(silent=True) or {}
    google_id = data.get("google_id")
    code = (data.get("code") or "").strip().upper()

    if not google_id or not code:
        return jsonify({"success": False, "message": "Missing google_id or coupon code"}), 400

    db = _get_db()
    if db is None:
        return jsonify({"success": False, "message": "Database not reachable"}), 503

    try:
        coupon = db[COUPONS_COLL].find_one_and_update(
            {"code": code, "used": False},
            {"$set": {"used": True, "redeemed_by": google_id, "redeemed_at": time.time()}},
            return_document=False
        )
        if not coupon:
            exists = db[COUPONS_COLL].find_one({"code": code})
            if exists:
                return jsonify({"success": False, "message": "This coupon has already been used."}), 400
            return jsonify({"success": False, "message": "Invalid coupon code."}), 400

        minutes = int(coupon.get("minutes", 0))
        if minutes <= 0:
            return jsonify({"success": False, "message": "Coupon has no time attached."}), 400

        added_seconds = minutes * 60
        db[COLL].update_one({"google_id": google_id}, {"$inc": {"credit_seconds": added_seconds}}, upsert=True)
        user = db[COLL].find_one({"google_id": google_id}, {"credit_seconds": 1})
        new_total = int((user or {}).get("credit_seconds", added_seconds))

        return jsonify({
            "success": True,
            "message": f"Added {minutes} minutes! Total: {new_total // 60} min {new_total % 60:02d} sec",
            "credit_seconds": new_total
        })
    except Exception as e:
        return jsonify({"success": False, "message": f"Redemption failed: {e}"}), 500


# ── Credit Helpers ────────────────────────────────────────────────────────────

def _get_credit_seconds(google_id: str) -> int:
    """Read current credit from MongoDB."""
    db = _get_db()
    if db is None or not google_id:
        return 0
    try:
        user = db[COLL].find_one({"google_id": google_id}, {"credit_seconds": 1})
        return int((user or {}).get("credit_seconds", 0))
    except Exception:
        return 0


def _add_credit_seconds(google_id: str, amount: int) -> int:
    """Atomically increment credit and return the new total."""
    db = _get_db()
    if db is None or not google_id:
        return 0
    try:
        result = db[COLL].find_one_and_update(
            {"google_id": google_id},
            {"$inc": {"credit_seconds": amount}},
            return_document=True,
            upsert=False,
        )
        return int((result or {}).get("credit_seconds", 0))
    except Exception as e:
        print(f"[Buy] add_credit_seconds error: {e}")
        return 0



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

    db = _get_db()
    if db is None:
        return jsonify({
            "success": False,
            "message": "Database unavailable"
        }), 503

    # -----------------------------------------
    # Find user
    # -----------------------------------------

    user = db[COLL].find_one({
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

        db["payments"].insert_one(payment)

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

    db = _get_db()
    if db is None:
        return jsonify({
            "success": False,
            "message": "Database unavailable"
        }), 503

    # =========================================
    # Find our payment record
    # =========================================

    payment = db["payments"].find_one({
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

    user = db[COLL].find_one({
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

    payment_update = db["payments"].update_one(

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
        latest_payment = db["payments"].find_one({
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

    db[COLL].update_one(

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

            db = _get_db()
            if db is None:
                return jsonify({"success": False, "message": "Database not reachable"}), 503

            payment = db["payments"].find_one({
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

            payment_update = db["payments"].update_one(

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

            db[COLL].update_one(

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

                db = _get_db()
                if db is not None:
                    db["payments"].update_one(

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
    sid          = request.sid
    data         = data or {}
    query_text   = data.get("query", "").strip()
    image_b64    = data.get("image", "")  # optional base64 PNG
    google_id    = data.get("google_id", "").strip()
    device_token = data.get("device_token", "").strip()

    if not query_text:
        emit("response_error", {"error": "No query provided"})
        return

    # ── Server-Side Auth & Credit Enforcement ────────────────────────────────
    db = _get_db()
    if db is not None:
        if not google_id:
            emit("response_error", {"error": "Authentication required. Please sign in with Google to use Aftaro."})
            return

        user = db[COLL].find_one({"google_id": google_id}, {"credit_seconds": 1, "current_device_token": 1})
        if not user:
            emit("response_error", {"error": "User account not found. Please sign in again."})
            return

        db_token = user.get("current_device_token", "")
        if device_token and db_token and db_token != device_token:
            emit("response_error", {"error": "Account is active on another device. Please sign in on this device."})
            return

        credit = int(user.get("credit_seconds", 0))
        if credit <= 0:
            emit("response_error", {"error": "Your interview credits have run out. Please buy credits to continue."})
            return

    if not _check_rate_limit(sid):
        emit("response_error", {
            "error": f"Rate limit exceeded — max {RATE_LIMIT} requests per {RATE_WINDOW_SEC}s. Please wait before sending another query."
        })
        return

    print(f"[Q] sid={sid[:8]}… [User={google_id[:8]}…] {'[IMG] ' if image_b64 else ''}{query_text[:80]}")
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