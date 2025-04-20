from langchain_openai import AzureChatOpenAI, AzureOpenAIEmbeddings

class AzureServices():
    def __init__(self, sets):
        self.AZURE_OPENAI_API_KEY = sets.azure_openai_api_key
        self.AZURE_OPENAI_ENDPOINT= sets.azure_openai_endpoint
        self.DEPLOYMENT_MODEL = sets.deployment_model
        self.EMBEGGIND_MODEL = sets.embedding_model
        self.llm = self.get_default_llm()


    def get_default_llm(self):
        return AzureChatOpenAI(openai_api_version="2023-07-01-preview",
                            azure_endpoint=self.AZURE_OPENAI_ENDPOINT,
                            openai_api_key=self.AZURE_OPENAI_API_KEY,
                            azure_deployment=self.DEPLOYMENT_MODEL,
                            temperature=0)
    
    def get_default_embedding(self):
        return AzureOpenAIEmbeddings(api_key=self.AZURE_OPENAI_API_KEY,
                                    api_version="2024-02-01",
                                    deployment=self.EMBEGGIND_MODEL,
                                    azure_endpoint=self.AZURE_OPENAI_ENDPOINT)