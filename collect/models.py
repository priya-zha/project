from django.db import models

GENDER_CHOICES=(('F','Femlae'),('M','Male'),('O','Other'))
MARRIAGE=(('N','No'),('Y','Yes'))

# Create your models here.
class Person(models.Model):
	name = models.CharField('Person Name', max_length=120)
	age = models.IntegerField()
	# sex =  models.CharField('Person Name', max_length=)
	#WBC_Count = models.IntegerField()
	RBC_Count = models.IntegerField(blank = True,null = True)
	Platelets = models.IntegerField(blank = True,null = True)
	Neutrofils = models.IntegerField(blank = True,null = True)
	Basofils = models.IntegerField(blank = True,null = True)
	glucose=models.FloatField(default=None)
	bloodpressure=models.FloatField(default=None)
	skinthickness = models.FloatField(default=None)
	insulin=models.FloatField(default=None)
	bmi=models.FloatField(default=None)
	maxrate =models.FloatField(default=None)

	# sex =  models.CharField('Person Name', max_length=)

	sex =  models.CharField(choices=GENDER_CHOICES, max_length=128, default=None)
	marital_status =  models.CharField(choices=MARRIAGE, max_length=128, default=None)
	cholestrol=models.FloatField('cholestoral in mg/dl',max_length=120, default=None)
	created_at = models.DateTimeField(auto_now_add=True)
