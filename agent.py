import os
import logging
import duckdb
import datetime
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain.agents import AgentExecutor
import warnings


from utils.azure_services import AzureServices
from utils.settings import Settings

warnings.filterwarnings('ignore')

class SQLAgent:
    def __init__(self, 
                 sets = Settings(), 
                 db_file:str = 'web_scrape_data.duckdb', 
                 table_name:str = 'books'):
        
        current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logging.basicConfig(
            level=logging.INFO,
            format=f'%(asctime)s - {current_date} - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        self.sets = sets
        self.client = AzureServices(sets = sets)
        self.llm = self.client.get_default_llm()
        self.db_file = db_file
        self.table_name = table_name
        self.db_uri = f"duckdb:///{os.path.abspath(db_file)}"
        self.db = SQLDatabase.from_uri(self.db_uri, 
                                       include_tables = [table_name], 
                                       sample_rows_in_table_info = 3)


    def create_agent(self):
        agent_executor: AgentExecutor = create_sql_agent(llm = self.llm,
                                                         db = self.db,
                                                         agent_type = "openai-tools",
                                                         verbose=True,
                                                         handle_parsing_errors = True,
                                                         handle_sql_errors=True,
                                                         top_k = 30, 
                                                         agent_executor_kwargs = {"return_intermediate_steps": True}
                                                         )
        
        return agent_executor


    def run(self, 
            question:str):
        agent_executor = self.create_agent()
        response = agent_executor.invoke({"input": question})

        query_sql = None
        for step in response.get("intermediate_steps", []):
            if isinstance(step, tuple) and len(step) >= 2:
                action = step[0]
                if hasattr(action, "tool") and action.tool == "sql_db_query":
                    query_sql = action.tool_input.get("query")
                    break
        return response["output"], query_sql