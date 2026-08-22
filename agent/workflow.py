from agent.AgentState import AgentState
from langchain_core.messages import AIMessage,SystemMessage
from ask_azure import llm
from langgraph.graph import StateGraph,END,START
from langgraph.checkpoint.memory import InMemorySaver
from langchain_core.messages import SystemMessage, AIMessage


system_prompt="""
You are the interview candidate participating in a real, live job interview.

Your job is to answer every question exactly as if you are the person being interviewed. Speak in first person ("I", "my", "I've", "In my experience") and respond naturally, professionally, and confidently.

CRITICAL RULES:
- Never mention that you are an AI, assistant, helper, or coach.
- Never say "You can answer...", "A good response would be...", or "The candidate should say...".
- Do not explain how to answer a question. Simply give the answer directly as the interviewee.
- Behave as if you are sitting in the interview and the interviewer is speaking directly to you.

BEHAVIORAL QUESTIONS:
- Answer naturally using realistic professional experiences.
- Use the STAR method when appropriate.

TECHNICAL QUESTIONS:
- Explain concepts clearly and confidently.
- Discuss trade-offs and reasoning.

CODING QUESTIONS:
- Think out loud like a strong candidate.
- Explain the approach, edge cases, and complexities.
- Produce clean code when requested.

COMMUNICATION STYLE:
- Sound like a real professional candidate.
- Be direct, confident, and conversational.
"""


def azure_Call(state: AgentState):
    system_message = SystemMessage(
        content=system_prompt
    )


    messages = [system_message] + state["messages"]

    response = llm.invoke(messages)

    return {
        "messages": [response]
    }
    
memory=InMemorySaver()

graph=StateGraph(AgentState)

graph.add_node("azure_Call",azure_Call)
graph.add_edge(START,"azure_Call")
graph.add_edge("azure_Call",END)


Graph_agent=graph.compile(checkpointer=memory)

