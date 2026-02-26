from django.contrib import admin
from .models import (Customer, Seller, Product, ProductCategoryTranslation,
                     Order, OrderItem, OrderPayment, OrderReview)

@admin.register(Customer)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('customer_id', 'customer_city', 'customer_state')
    list_filter = ('customer_state',)
    search_fields = ('customer_id', 'customer_city')

@admin.register(Seller)
class SellerAdmin(admin.ModelAdmin):
    list_display = ('seller_id', 'seller_city', 'seller_state')
    list_filter = ('seller_state',)

@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display = ('product_id', 'product_category_name', 'product_weight_g')
    list_filter = ('product_category_name',)

@admin.register(Order)
class OrderAdmin(admin.ModelAdmin):
    list_display = ('order_id', 'customer', 'order_status', 'order_purchase_timestamp')
    list_filter = ('order_status',)

@admin.register(OrderItem)
class OrderItemAdmin(admin.ModelAdmin):
    list_display = ('order', 'product', 'seller', 'price', 'freight_value')

@admin.register(OrderPayment)
class OrderPaymentAdmin(admin.ModelAdmin):
    list_display = ('order', 'payment_type', 'payment_installments', 'payment_value')
    list_filter = ('payment_type',)

@admin.register(OrderReview)
class OrderReviewAdmin(admin.ModelAdmin):
    list_display = ('order', 'review_score', 'review_creation_date')
    list_filter = ('review_score',)
