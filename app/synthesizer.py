import sys
import json
import os
from typing import List, Dict, Any
from datetime import datetime

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
    llm = ChatOllama(
        base_url="https://inference.ccrolabs.com",
        model="llama3", # Replace with your desired Ollama model
        temperature=0.7,
        format="json" # Instruct Ollama to output in JSON format
    )

    # 2. Create a JSON Output Parser
    parser = JsonOutputParser(pydantic_object=GeneratedData)

    # 3. Construct a detailed prompt for the LLM
    prompt_template = """
    You are a world-class data fabrication engine. Your purpose is to generate highly realistic, diverse, and structured synthetic data.

    **Task:**
    Generate exactly {num_records} distinct data records that strictly conform to the provided JSON schema.

    **Contextual Information for Realism:**
    - Current Date: {current_date}
    - Location Context: Cluj-Napoca, Romania (generate data that would be plausible for this region where applicable, e.g., names, addresses).

    **Instructions:**
    1.  **Strict Schema Adherence:** Every field in every generated record must match the type and format specified in the schema. Pay close attention to the descriptions for contextual clues.
    2.  **Data Diversity:** Ensure the generated records are unique and cover a wide range of plausible values. Avoid simple repetitions. For example, if generating user data, create different names, ages, and emails for each record.
    3.  **Realism:** The generated data should look like real-world information. Use the contextual information provided to enhance realism.
    4.  **JSON Output Only:** Your entire output must be a single, valid JSON object that matches the format instructions below. Do not include any introductory text, explanations, apologies, or markdown formatting like ```json.

    **JSON Schema to follow:**
    ```json
    {schema}
    ```

    **Required Output Format:**
    {format_instructions}
    """
    
    prompt = ChatPromptTemplate.from_template(
        template=prompt_template,
        partial_variables={"format_instructions": parser.get_format_instructions()},
    )

    # 4. Create the LangChain chain
    chain = prompt | llm | parser

    # 5. Invoke the chain to generate data
    print("Generating data with Ollama... This may take a moment.", file=sys.stderr)
    
    # Get current date for the prompt
    current_date = datetime.now().strftime("%Y-%m-%d")

    result = chain.invoke({
        "num_records": num_records,
        "schema": json.dumps(schema, indent=2),
        "current_date": current_date
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
