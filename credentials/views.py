import json
import tempfile
import os
import base64
from django.shortcuts import render
from django.http import JsonResponse, HttpResponse
from .forms import GoogleCredentialForm
from .services import GoogleAuthService

# Allow OAuth lib to use http for development
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

def index(request):
    form = GoogleCredentialForm()
    
    if request.method == 'POST':
        # Step 1: Get Authorization URL
        if 'get_url' in request.POST:
            if request.FILES.get('config_file'):
                config_file = request.FILES['config_file']
                file_content = config_file.read()
                file_base64 = base64.b64encode(file_content).decode('utf-8')
                
                with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as tmp:
                    tmp.write(file_content)
                    tmp_path = tmp.name
                
                try:
                    auth_url, code_verifier = GoogleAuthService.get_auth_url(tmp_path)
                    return JsonResponse({
                        'auth_url': auth_url,
                        'file_data': file_base64,
                        'code_verifier': code_verifier  # Send back to client for persistence
                    })
                except Exception as e:
                    return JsonResponse({'error': str(e)}, status=400)
                finally:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
            else:
                return JsonResponse({'error': 'Please upload a credentials.json file first.'}, status=400)

        # Step 2: Generate Token
        elif 'generate_token' in request.POST:
            response_url = request.POST.get('response_url')
            file_data_base64 = request.POST.get('file_data_base64')
            code_verifier = request.POST.get('code_verifier')
            
            # Recover file content
            if not file_data_base64 and request.FILES.get('config_file'):
                file_content = request.FILES['config_file'].read()
            elif file_data_base64:
                file_content = base64.b64decode(file_data_base64)
            else:
                return JsonResponse({'error': 'Credentials file missing. Please re-upload.'}, status=400)

            if not code_verifier:
                return JsonResponse({'error': 'Code verifier missing. Please click "Get Auth URL" again.'}, status=400)

            if file_content and response_url:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as tmp:
                    tmp.write(file_content)
                    tmp_path = tmp.name
                
                try:
                    token_data = GoogleAuthService.generate_token_from_url(
                        tmp_path, response_url, code_verifier
                    )
                    return JsonResponse({
                        'success': True,
                        'token_data': token_data
                    })
                except Exception as e:
                    return JsonResponse({'error': str(e)}, status=400)
                finally:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
            else:
                return JsonResponse({'error': 'Missing callback URL.'}, status=400)

    return render(request, 'credentials/index.html', {'form': form})
