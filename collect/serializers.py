from rest_framework import serializers

from .models import Person

class PersonSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Person
        fields = ('name','age','sex','RBC_Count','Platelets','Neutrofils','Basofils','glucose','bloodpressure','skinthickness','insulin','bmi','maxrate','marital_status','cholestrol','created_at'
)


	