from openai import OpenAI
import logging
from .models import LLMSettings

logger = logging.getLogger(__name__)

class LLMService:
    @staticmethod
    def get_client():
        """
        Initializes the OpenAI client with stored settings.
        If no settings found, returns None or raises exception.
        """
        config = LLMSettings.objects.first()
        if not config or not config.api_key:
            logger.error("LLM Settings (API Key) not found in database.")
            return None
        
        # Initialize the OpenAI client
        logger.debug(f"Initializing OpenAI client with base_url: {config.base_url}")
        client = OpenAI(
            api_key=config.api_key,
            base_url=config.base_url
        )
        return client

    @staticmethod
    def generate_completion(prompt, system_prompt="You are a helpful assistant."):
        """
        Simple method to get a text completion.
        """
        client = LLMService.get_client()
        if not client:
            raise Exception("LLM Settings not configured correctly in the admin.")

        config = LLMSettings.objects.first()
        
        logger.info(f"Generating LLM completion using model: {config.model_name}")
        try:
            response = client.chat.completions.create(
                model=config.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
            )
            logger.debug("LLM response received successfully.")
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Error during LLM completion: {e}", exc_info=True)
            raise
