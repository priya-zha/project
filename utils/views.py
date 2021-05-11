from django.shortcuts import render

# Create your views here.
def home(request):
    plot_url = "This is the plot url"
    context = {
        "page_title": "| Home",
    }
    return render(request, "utils/home.html", context)

