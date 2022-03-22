import datetime

import calendar
from typing import ClassVar, Final, List

from django.conf import settings
from django import forms
from django.forms import TextInput, JSONField, CharField

from eo_engine.common.contrib.waporv2 import well_known_bboxes
from .models import Credentials
from .models.other import CrawlerConfiguration


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


class RunTaskForm(forms.Form):
    task_name = CharField(max_length=255)
    task_kwargs = JSONField()

    def clean_task_name(self):
        # This method is not passed any parameters.
        # You will need to look up the value of the field in self.cleaned_data
        # and remember that it will be a Python object at this point
        from .common.tasks import get_task_ref_from_name
        task_name = self.cleaned_data['task_name']
        normalised_task_name = get_task_ref_from_name(task_name)
        return normalised_task_name


class WaporForm(forms.Form):
    from_date = forms.DateField(required=True,
                                widget=forms.widgets.DateInput(
                                    attrs={'type': 'date',
                                           'min': settings.FORMS_MIN_DATE,
                                           'max': datetime.date.today().strftime('%Y-%m-%d')}))
    to_date = forms.DateField(required=True,
                              widget=forms.widgets.DateInput(
                                  attrs={'type': 'date',
                                         'min': settings.FORMS_MIN_DATE,
                                         'max': datetime.date.today().strftime('%Y-%m-%d')}))


class WaporNdviForm(forms.Form):
    level = forms.ChoiceField(required=True, choices=(('L1', 'Level-1'), ('L2', 'Level-2')),
                              initial='L2')
    dekad = forms.ChoiceField(required=True, choices=(
        (1, '1st Dekad'),
        (2, '2st Dekad'),
        (3, '3st Dekad'),
    ), widget=forms.RadioSelect(), initial=1)

    month = forms.ChoiceField(required=True, choices=[(i, calendar.month_abbr[i]) for i in range(1, 13)])
    year = forms.ChoiceField(required=True, choices=[(i, i) for i in range(2008, 2021 + 1)])

    prod_name: ClassVar[str] = 'QUAL_NDVI'
    dimension: Final[str] = 'D'  # dekad
    area = forms.ChoiceField(
        required=True,
        choices=[(s, s) for s in well_known_bboxes.keys()], widget=forms.RadioSelect(),
        initial=list(well_known_bboxes.keys())[0])

    def clean(self):
        cleaned_data = super().clean()
        level = cleaned_data.get('level')
        area = cleaned_data.get('area')
        if area == 'africa' and level == 'L2':
            self.add_error('level', "You can only select Level-1 if Africa is selected")


class SimpleYearDropDownForm(forms.Form):

    def __init__(self, *args, **kwargs):
        self.from_year = 2000
        self.to_year = 2025
        if 'from_year' in kwargs.keys():
            self.from_year = kwargs.pop('from_year')
        if 'to_year' in kwargs.keys():
            self.from_year = kwargs.pop('to_year')
        super(SimpleYearDropDownForm, self).__init__(*args, **kwargs)
        self.choices: List[List[str, str]] = []
        for year in range(self.from_year, self.to_year + 1):
            self.choices.append([str(year), str(year)])

        field = forms.DateField(
            required=True,
            widget=forms.Select(choices=self.choices),
            input_formats=['%Y'])
        self.fields['from_date'] = field


class SimpleFromDateForm(forms.Form):
    from_date = forms.DateField(
        required=True,
        widget=forms.widgets.DateInput(
            attrs={'type': 'date',
                   'min': settings.FORMS_MIN_DATE,
                   'max': datetime.date.today().strftime('%Y-%m-%d')}))


class SimpleFromToDateForm(SimpleFromDateForm):
    to_date = forms.DateField(
        required=True,
        widget=forms.widgets.DateInput(
            attrs={'type': 'date',
                   'min': settings.FORMS_MIN_DATE,
                   'max': datetime.date.today().strftime('%Y-%m-%d')}))


class EOSourceMetaForm(forms.ModelForm):
    class Meta:
        model = CrawlerConfiguration
        fields = ['enabled', 'from_date', ]
        widgets = {
            'from_date': forms.DateInput(attrs={'type': 'date'})
        }
