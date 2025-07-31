import sys
import json
import os
from typing import List, Dict, Any

# Use ChatOllama for Ollama integration
from langchain_community.chat_models import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.pydantic_v1 import BaseModel, Field
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Pydantic Model for Structured Output ---
# We define the structure we want the LLM to return. This helps ensure
# we get a clean, predictable JSON list.
class GeneratedData(BaseModel):
    records: List[Dict[str, Any]] = Field(description="A list of generated data records conforming to the schema.")

def generate_data_with_llm(schema: Dict[str, Any], num_records: int) -> List[Dict[str, Any]]:
    """
    This function uses a Large Language Model via Ollama to generate synthetic data
    that conforms to a given JSON schema.
    """
    # 1. Initialize the Ollama Language Model
    # It connects to the specified endpoint. Make sure to specify the model
    # you want to use (e.g., 'llama3', 'mistral', etc.).
    llm = ChatOllama(
        base_url="https://inference.ccrolabs.com",
        model="llama3", # Replace with your desired Ollama model
        temperature=0.7,
        format="json" # Instruct Ollama to output in JSON format
    )

    # 2. Create a JSON Output Parser
    # This parser will automatically try to fix any malformed JSON from the LLM.
    parser = JsonOutputParser(pydantic_object=GeneratedData)

    # 3. Construct a detailed prompt for the LLM
    prompt_template = """
    You are an expert synthetic data generator. Your task is to generate a list of {num_records} realistic data records that strictly adhere to the following JSON schema.
    The data should be diverse and contextually relevant based on the field names and descriptions.
    Your output MUST be a valid JSON object that follows the provided format instructions.

    JSON Schema:
    ```json
    {schema}
    ```

    {format_instructions}
    """
    
    prompt = ChatPromptTemplate.from_template(
        template=prompt_template,
        partial_variables={"format_instructions": parser.get_format_instructions()},
    )

    # 4. Create the LangChain chain
    # This chain pipes the prompt to the LLM and then parses the output.
    chain = prompt | llm | parser

    # 5. Invoke the chain to generate data
    print("Generating data with Ollama... This may take a moment.", file=sys.stderr)
    result = chain.invoke({
        "num_records": num_records,
        "schema": json.dumps(schema, indent=2)
    })
    
    return result.get('records', [])

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python synthesizer.py '<json_schema>' <num_records>", file=sys.stderr)
        sys.exit(1)

    try:
        input_schema = json.loads(sys.argv[1])
        num_records = int(sys.argv[2])
        
        # --- CORE LOGIC ---
        # This is the main call to the LLM-based data generation function.
        final_data = generate_data_with_llm(input_schema, num_records)
        
        # Print the final JSON data to standard output
        print(json.dumps(final_data, indent=2))
        
    except json.JSONDecodeError:
        print("Error: Invalid JSON schema provided.", file=sys.stderr)
        sys.exit(1)
    except ValueError:
        print("Error: Invalid number of records provided.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)
