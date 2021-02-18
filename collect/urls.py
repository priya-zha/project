from django.urls import include, path
from .views import app1, PersonViewSet

from rest_framework import routers

router = routers.DefaultRouter()
router.register(r'Person', PersonViewSet)

app_name = 'collect'
urlpatterns = [
    path('', app1, name='app1'),
    path('api/', include(router.urls)),
    path('api-auth/', include('rest_framework.urls', namespace='rest_framework'))
   
]
