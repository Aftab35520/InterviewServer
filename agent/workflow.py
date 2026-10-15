from agent.AgentState import AgentState
from langchain_core.messages import AIMessage,SystemMessage,RemoveMessage
from ask_azure import llm
from langgraph.graph import StateGraph,END,START
from langgraph.checkpoint.memory import InMemorySaver


system_prompt = """
You are the candidate in a live job interview.

Answer every question in first person ("I", "my", "I've") as if you are speaking directly to the interviewer.

Rules:
- Never mention AI or that you're an assistant.
- Never explain how to answer; answer directly as the candidate.
- Be confident, professional, natural, and conversational.

Behavioral:
- Use the STAR method when appropriate.

Technical:
- Explain concepts clearly and discuss trade-offs.

Coding:
- Think aloud, explain your approach, edge cases, time/space complexity, and provide clean code when requested.
"""


def summarize(messages, previous_summary=""):
    prompt = f"""
You are maintaining a running summary of an interview.

Existing summary:
{previous_summary}

Update the summary using the new conversation.

Keep:
- Candidate background
- Skills & technologies
- Behavioural answers
- Coding discussion
- Interviewer's preferences
- Important unresolved topics

Return only the updated summary.
"""

    response = llm.invoke(
        [SystemMessage(content=prompt)] + messages
    )

    return response.content


max_history = 20
keep_last = 6


def azure_Call(state: AgentState):
    system_message = SystemMessage(content=system_prompt)

    messages = state["messages"]
    summary = state.get("summary", "")

    # -------------------------
    # Summarize if history is long
    # -------------------------
    if len(messages) > max_history:

        new_summary = summarize(
            messages[:-keep_last],
            previous_summary=summary,
        )

        # Delete old messages from checkpoint
        deletes = [
            RemoveMessage(id=m.id)
            for m in messages[:-keep_last]
            if getattr(m, "id", None)
        ]

        prompt_messages = [
            system_message,
            SystemMessage(
                content=f"Conversation summary:\n{new_summary}"
            ),
            *messages[-keep_last:]
        ]

        response = llm.invoke(prompt_messages)
        print(f"In: {response.usage_metadata['input_tokens']} | Out: {response.usage_metadata['output_tokens']} | Total: {response.usage_metadata['total_tokens']}")
        return {
            "messages": deletes + [response],
            "summary": new_summary,
        }

    # -------------------------
    # Normal conversation
    # -------------------------
    prompt_messages = [system_message]

    if summary:
        prompt_messages.append(
            SystemMessage(
                content=f"Conversation summary:\n{summary}"
            )
        )

    prompt_messages.extend(messages)

    response = llm.invoke(prompt_messages)
    print(f"In: {response.usage_metadata['input_tokens']} | Out: {response.usage_metadata['output_tokens']} | Total: {response.usage_metadata['total_tokens']}")
    return {
        "messages": [response],
        "summary": summary,
    }


    
    
memory=InMemorySaver()

graph=StateGraph(AgentState)

graph.add_node("azure_Call",azure_Call)
graph.add_edge(START,"azure_Call")
graph.add_edge("azure_Call",END)


Graph_agent=graph.compile(checkpointer=memory)


