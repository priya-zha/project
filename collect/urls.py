from django.urls import include, path
from .views import app1, PersonViewSet, plot1, plot2,plot3,plot4,plot5,plotEDA

from rest_framework import routers

router = routers.DefaultRouter()
router.register(r'Person', PersonViewSet)

app_name = 'collect'
urlpatterns = [
    path('', app1, name='app1'),
    path('plot1/',plot1, name='plot1'),
    path('plot2/',plot2, name='plot2'),
    path('plot3/',plot3, name='plot3'),
    path('plot4/',plot4, name='plot4'),
    path('plot5/',plot5, name='plot5'),
    path('plotEDA/',plotEDA, name='plotEDA'),
    path('api/', include(router.urls)),
    path('api-auth/', include('rest_framework.urls', namespace='rest_framework'))
   
]
