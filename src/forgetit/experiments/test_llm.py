from forgetit.llm import get_llm, OpenAiLLM


llm: OpenAiLLM = get_llm()



print(llm.generate("Say 'pong' and nothing else.", max_tokens=10, temperature=0))
