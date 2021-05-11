from django.conf import settings
from django.urls import path
from .views import home

app_name = 'utils'
urlpatterns = [
            path('', home, name='home'),
            ]
