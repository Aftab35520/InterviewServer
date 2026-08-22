from langchain_core.messages import HumanMessage
from agent.workflow import Graph_agent


def agent_call(query, thread_id=1):
    config = {
        "configurable": {
            "thread_id": str(thread_id)
        }
    }
    query_message = HumanMessage(content=query)

    for message_chunk, _ in Graph_agent.stream(
        {"messages": [query_message]},
        stream_mode="messages",
        config=config,
    ):
        if message_chunk.content:
            yield message_chunk.content
