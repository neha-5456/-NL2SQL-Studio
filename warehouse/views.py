import json
import logging
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.conf import settings
from .nl2sql_engine import NL2SQLEngine, DemoNL2SQLEngine
from .models import Customer, Order, Product, Seller, OrderItem, OrderPayment, OrderReview

logger = logging.getLogger(__name__)


def index(request):
    """Main page."""
    stats = {
        'total_customers': Customer.objects.count(),
        'total_orders': Order.objects.count(),
        'total_products': Product.objects.count(),
        'total_sellers': Seller.objects.count(),
        'total_order_items': OrderItem.objects.count(),
        'total_payments': OrderPayment.objects.count(),
        'total_reviews': OrderReview.objects.count(),
        'has_api_key': bool(settings.ANTHROPIC_API_KEY),
    }
    return render(request, 'index.html', {'stats': stats})


@csrf_exempt
@require_http_methods(["POST"])
def query_api(request):
    """POST /api/query/ — NL to SQL endpoint."""
    try:
        body = json.loads(request.body)
        question = body.get('question', '').strip()

        if not question:
            return JsonResponse({'success': False, 'error': 'Please provide a question.'}, status=400)
        if len(question) > 500:
            return JsonResponse({'success': False, 'error': 'Max 500 characters.'}, status=400)

        if settings.ANTHROPIC_API_KEY:
            engine = NL2SQLEngine()
        else:
            engine = DemoNL2SQLEngine()

        result = engine.process_question(question)
        return JsonResponse(result)

    except json.JSONDecodeError:
        return JsonResponse({'success': False, 'error': 'Invalid JSON.'}, status=400)
    except Exception as e:
        logger.error(f"Query API error: {str(e)}")
        return JsonResponse({'success': False, 'error': f'Server error: {str(e)}'}, status=500)


@require_http_methods(["GET"])
def schema_api(request):
    """GET /api/schema/"""
    schema = {
        'tables': [
            {'name': 'olist_customers', 'rows': Customer.objects.count()},
            {'name': 'olist_sellers', 'rows': Seller.objects.count()},
            {'name': 'olist_products', 'rows': Product.objects.count()},
            {'name': 'olist_orders', 'rows': Order.objects.count()},
            {'name': 'olist_order_items', 'rows': OrderItem.objects.count()},
            {'name': 'olist_order_payments', 'rows': OrderPayment.objects.count()},
            {'name': 'olist_order_reviews', 'rows': OrderReview.objects.count()},
        ]
    }
    return JsonResponse(schema)
