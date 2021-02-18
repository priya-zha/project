from django.shortcuts import render
from django.shortcuts import render, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework import viewsets
from time import sleep
from .serializers import PersonSerializer


from .models import Person
# Create your views here.
# queryset=''

@csrf_exempt
def app1(request):
    if request.method == "POST":
        print(request.POST)
        name = request.POST.get('name')
        age = request.POST.get('age')
        glucose = request.POST.get('glucose')
        bloodpressure = request.POST.get('bloodpressure')
        skinthickness = request.POST.get('skinthickness')
        insulin = request.POST.get('insulin')
        bmi = request.POST.get('bmi')
        maxrate = request.POST.get('max_heart_rate')
        sex=request.POST.get('sex')
        marital_status=request.POST.get('marital_status')
        cholestrol=request.POST.get('cholestrol')
        RBC_Count = request.POST.get('RBC_Count')
        Platelets  = request.POST.get('Platelets')
        Neutrofils = request.POST.get('Neutrofils')
        Basofils = request.POST.get('Basofils')


        
        if name and age:
            obj = Person.objects.create(name=name, age=age,RBC_Count=RBC_Count,Platelets=Platelets, Neutrofils=Neutrofils,Basofils=Basofils, glucose=glucose, sex=sex, marital_status=marital_status, cholestrol=cholestrol, bloodpressure=bloodpressure, skinthickness=skinthickness, insulin=insulin, bmi=bmi, maxrate=maxrate)
            obj.save()




    return render(request, 'collect/home.html')

# def update_data():
#     global queryset
#     queryset = Person.objects.all().order_by('created_at').reverse()
#     return queryset

class PersonViewSet(viewsets.ModelViewSet):
    queryset = Person.objects.all().order_by('created_at').reverse()
    # while True:
    #     sleep(1)
    #     update_data()
    # global queryset 
    serializer_class = PersonSerializer



