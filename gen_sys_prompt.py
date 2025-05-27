from openai import OpenAI

def load_api_key(file_path: str) -> str:
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.readline().strip()

client = OpenAI(api_key=load_api_key("api_key/deepseek.txt"), base_url="https://api.deepseek.com")

abstract = "In a search for genes that regulate circadian rhythms in mammals, the progeny of mice treated with N -ethyl- N -nitrosourea (ENU) were screened for circadian clock mutations. A semidominant mutation, Clock , that lengthens circadian period and abolishes persistence of rhythmicity was identified. Clock segregated as a single gene that mapped to the midportion of mouse chromosome 5, a region syntenic to human chromosome 4. The power of ENU mutagenesis combined with the ability to clone murine genes by map position provides a generally applicable approach to study complex behavior in mammals."
annotation = "This study was the first to successfully use random mutagenesis, phenotype-based screening and positional cloning (that is, a forward-genetic approach) in mice."

completion = client.chat.completions.create(
    model="deepseek-chat",
    messages=[
        {
            "role": "system",
            "content": "You are an expert in prompt engineering for large language models. Based on the user's needs, write a system prompt for an intelligent assistant to guide content generation. Requirements:\n1. Output will be used as the system message in the beginning of a few-shot learning conversation. \n2. Align with user needs, describe the assistant's positioning, capabilities, and knowledge base\n3. The prompt should be clear, precise, and easy to understand, as concise as possible while maintaining quality\n4. Output only the prompt, do not include extra explanations"
        },
        {
            "role": "user",
            "content": f"Please help me generate a prompt for a 'bibliography annotation' task. The assistant should be able to summarize the provided abstract in a concise and clear manner, highlighting the main points and findings, and generating high-quality annotations. The assistant should also be knowledgeable about scientific literature and able to understand complex terminology. A sample abstract-annotation pair is as follows:\nAbstract: '{abstract}'\nAnnotation: '{annotation}'. The assistant should output the generated annotation only. "
        }
    ],
    stream=False
)

content = completion.choices[0].message.content or ""
print(content)
with open("sys_prompt.txt", "w", encoding="utf-8") as f:
    f.write(content)