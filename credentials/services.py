import json
import os
from google_auth_oauthlib.flow import Flow
from urllib.parse import urlparse, parse_qs

class GoogleAuthService:
    SCOPES = [
        'https://www.googleapis.com/auth/drive',
    ]

    @staticmethod
    def get_auth_url(credentials_file_path):
        """
        Generates the authorization URL and returns both the URL and the code_verifier.
        The code_verifier MUST be saved and reused during token exchange.
        """
        flow = Flow.from_client_secrets_file(
            credentials_file_path,
            scopes=GoogleAuthService.SCOPES,
            redirect_uri='http://localhost'
        )
        # Manually generate code_verifier so we can capture it
        flow.code_verifier = flow.oauth2session.new_state()
        import hashlib, base64
        code_challenge = base64.urlsafe_b64encode(
            hashlib.sha256(flow.code_verifier.encode('ascii')).digest()
        ).rstrip(b'=').decode('ascii')
        
        auth_url, state = flow.authorization_url(
            prompt='consent', 
            access_type='offline',
            code_challenge=code_challenge,
            code_challenge_method='S256'
        )
        
        return auth_url, flow.code_verifier

    @staticmethod
    def generate_token_from_url(credentials_file_path, authorization_response_url, code_verifier):
        """
        Exchange the authorization code for a token.
        The code_verifier from the initial auth step MUST be provided.
        """
        flow = Flow.from_client_secrets_file(
            credentials_file_path,
            scopes=GoogleAuthService.SCOPES,
            redirect_uri='http://localhost'
        )
        # Restore the code_verifier from the original auth request
        flow.code_verifier = code_verifier
        
        authorization_response_url = authorization_response_url.strip()
        
        flow.fetch_token(authorization_response=authorization_response_url)
        
        creds = flow.credentials
        
        token_data = {
            'token': creds.token,
            'refresh_token': creds.refresh_token,
            'token_uri': creds.token_uri,
            'client_id': creds.client_id,
            'client_secret': creds.client_secret,
            'scopes': list(creds.scopes) if creds.scopes else [],
            'expiry': creds.expiry.isoformat() if creds.expiry else None
        }
        
        return token_data

    @staticmethod
    def refresh_access_token(refresh_token, client_id, client_secret, token_uri='https://oauth2.googleapis.com/token'):
        """
        Refreshes the access token using the refresh token.
        Returns a dictionary with the new access token and its expiry.
        """
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials

        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri=token_uri,
            client_id=client_id,
            client_secret=client_secret
        )

        # Refresh the token
        creds.refresh(Request())

        return {
            'token': creds.token,
            'expiry': creds.expiry.isoformat() if creds.expiry else None
        }
