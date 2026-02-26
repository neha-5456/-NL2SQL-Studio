from django.db import models


class Customer(models.Model):
    """olist_customers_dataset.csv"""
    customer_id = models.CharField(max_length=50, primary_key=True)
    customer_unique_id = models.CharField(max_length=50, db_index=True)
    customer_zip_code_prefix = models.CharField(max_length=10)
    customer_city = models.CharField(max_length=100)
    customer_state = models.CharField(max_length=5)

    class Meta:
        db_table = 'olist_customers'

    def __str__(self):
        return f"{self.customer_id} ({self.customer_city})"


class Seller(models.Model):
    """olist_sellers_dataset.csv"""
    seller_id = models.CharField(max_length=50, primary_key=True)
    seller_zip_code_prefix = models.CharField(max_length=10)
    seller_city = models.CharField(max_length=100)
    seller_state = models.CharField(max_length=5)

    class Meta:
        db_table = 'olist_sellers'

    def __str__(self):
        return f"{self.seller_id} ({self.seller_city})"


class Product(models.Model):
    """olist_products_dataset.csv"""
    product_id = models.CharField(max_length=50, primary_key=True)
    product_category_name = models.CharField(max_length=100, blank=True, null=True)
    product_name_length = models.IntegerField(null=True, blank=True)
    product_description_length = models.IntegerField(null=True, blank=True)
    product_photos_qty = models.IntegerField(null=True, blank=True)
    product_weight_g = models.IntegerField(null=True, blank=True)
    product_length_cm = models.IntegerField(null=True, blank=True)
    product_height_cm = models.IntegerField(null=True, blank=True)
    product_width_cm = models.IntegerField(null=True, blank=True)

    class Meta:
        db_table = 'olist_products'

    def __str__(self):
        return f"{self.product_id} ({self.product_category_name})"


class ProductCategoryTranslation(models.Model):
    """product_category_name_translation.csv"""
    product_category_name = models.CharField(max_length=100, primary_key=True)
    product_category_name_english = models.CharField(max_length=100)

    class Meta:
        db_table = 'product_category_translation'

    def __str__(self):
        return f"{self.product_category_name} → {self.product_category_name_english}"


class Geolocation(models.Model):
    """olist_geolocation_dataset.csv"""
    geolocation_zip_code_prefix = models.CharField(max_length=10, db_index=True)
    geolocation_lat = models.FloatField()
    geolocation_lng = models.FloatField()
    geolocation_city = models.CharField(max_length=100)
    geolocation_state = models.CharField(max_length=5)

    class Meta:
        db_table = 'olist_geolocation'

    def __str__(self):
        return f"{self.geolocation_zip_code_prefix} - {self.geolocation_city}"


class Order(models.Model):
    """olist_orders_dataset.csv"""
    order_id = models.CharField(max_length=50, primary_key=True)
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE, related_name='orders',
                                 db_column='customer_id')
    order_status = models.CharField(max_length=20)
    order_purchase_timestamp = models.DateTimeField(null=True, blank=True)
    order_approved_at = models.DateTimeField(null=True, blank=True)
    order_delivered_carrier_date = models.DateTimeField(null=True, blank=True)
    order_delivered_customer_date = models.DateTimeField(null=True, blank=True)
    order_estimated_delivery_date = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'olist_orders'

    def __str__(self):
        return f"{self.order_id} ({self.order_status})"


class OrderItem(models.Model):
    """olist_order_items_dataset.csv"""
    order = models.ForeignKey(Order, on_delete=models.CASCADE, related_name='items',
                              db_column='order_id')
    order_item_id = models.IntegerField()
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name='order_items',
                                db_column='product_id')
    seller = models.ForeignKey(Seller, on_delete=models.CASCADE, related_name='order_items',
                               db_column='seller_id')
    shipping_limit_date = models.DateTimeField(null=True, blank=True)
    price = models.DecimalField(max_digits=10, decimal_places=2)
    freight_value = models.DecimalField(max_digits=10, decimal_places=2)

    class Meta:
        db_table = 'olist_order_items'

    def __str__(self):
        return f"Order {self.order_id} - Item {self.order_item_id}"


class OrderPayment(models.Model):
    """olist_order_payments_dataset.csv"""
    order = models.ForeignKey(Order, on_delete=models.CASCADE, related_name='payments',
                              db_column='order_id')
    payment_sequential = models.IntegerField()
    payment_type = models.CharField(max_length=30)
    payment_installments = models.IntegerField()
    payment_value = models.DecimalField(max_digits=10, decimal_places=2)

    class Meta:
        db_table = 'olist_order_payments'

    def __str__(self):
        return f"Order {self.order_id} - {self.payment_type} R${self.payment_value}"


class OrderReview(models.Model):
    """olist_order_reviews_dataset.csv"""
    review_id = models.CharField(max_length=50)
    order = models.ForeignKey(Order, on_delete=models.CASCADE, related_name='reviews',
                              db_column='order_id')
    review_score = models.IntegerField()
    review_comment_title = models.TextField(blank=True, null=True)
    review_comment_message = models.TextField(blank=True, null=True)
    review_creation_date = models.DateTimeField(null=True, blank=True)
    review_answer_timestamp = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'olist_order_reviews'

    def __str__(self):
        return f"Review {self.review_id} ({self.review_score}★)"
