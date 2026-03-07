from django import forms

class GoogleCredentialForm(forms.Form):
    config_file = forms.FileField(
        label="Upload credentials.json",
        help_text="Upload your Google OAuth credentials.json file",
        widget=forms.FileInput(attrs={
            'class': 'hidden',
            'id': 'file_upload',
            'accept': '.json'
        })
    )
    response_url = forms.CharField(
        label="Callback URL",
        required=False,
        widget=forms.TextInput(attrs={
            'placeholder': 'Paste the localhost URL from browser here...',
            'class': 'w-full bg-black/40 border border-gray-700 rounded-xl px-4 py-3 outline-none focus:border-blue-500 font-mono text-sm'
        })
    )
