from django import forms
from django.forms import Textarea, TextInput

from .models import Credentials


class CredentialsUsernamePassowordForm(forms.ModelForm):
    # def __init__(self, *args, **kwargs):
    #     super().__init__(*args, **kwargs)
    #     self.fields['name'].widget.attrs.update({'class': 'special'})
    #     self.fields['comment'].widget.attrs.update(size='40')

    class Meta:
        model = Credentials
        fields = ['domain', 'username', 'password']
        widgets = {
            'domain': TextInput(attrs={'cols': 80, 'rows': 20}),
        }


class CredentialsAPIKEYForm(forms.ModelForm):
    class Meta:
        model = Credentials
        fields = ['domain', 'api_key']
