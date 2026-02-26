from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('api/query/', views.query_api, name='query_api'),
    path('api/schema/', views.schema_api, name='schema_api'),
]
