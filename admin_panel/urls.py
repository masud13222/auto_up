from django.urls import path
from django.contrib.auth import views as auth_views
from . import views

app_name = 'panel'

urlpatterns = [
    # Auth
    path('login/', auth_views.LoginView.as_view(template_name='admin/login.html'), name='login'),
    path('logout/', views.logout_view, name='logout'),

    # Core pages
    path('', views.dashboard, name='dashboard'),
    path('process/', views.process, name='process'),
    path('queue/', views.queue, name='queue'),
    path('auto-up/skip-urls/', views.auto_up_skip_urls, name='auto_up_skip_urls'),
    path('settings/', views.settings_view, name='settings'),
    path('google-accounts/', views.google_accounts, name='google_accounts'),
    path('llm/', views.llm_settings, name='llm_settings'),
    path('llm/chatbot/', views.llm_chat, name='llm_chat'),
    path('llm/chat/', views.llm_chat_api, name='llm_chat_api'),

    # Task actions
    path('task/<int:pk>/', views.task_detail, name='task_detail'),
    path('task/<int:pk>/status/', views.task_status_api, name='task_status_api'),
    path('task/<int:pk>/requeue/', views.requeue_task, name='requeue_task'),
    path('task/<int:pk>/delete/', views.delete_task, name='delete_task'),

    # Google account actions
    path('google-accounts/add/', views.add_google_account, name='add_google_account'),
    path('google-accounts/<int:pk>/delete/', views.delete_google_account, name='delete_google_account'),

    # HTMX fragments
    path('fragments/recent-tasks/', views.recent_tasks_fragment, name='recent_tasks_fragment'),
    path('fragments/queue-status/', views.queue_status_api, name='queue_status_api'),
]
