from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.views.generic import RedirectView

urlpatterns = [
    path('admin/', admin.site.urls),
    path('panel/', include('admin_panel.urls')),
    path('credentials/', include('credentials.urls')),
    path('upload/', include('upload.urls')),
    path('', RedirectView.as_view(url='/panel/', permanent=False)),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
