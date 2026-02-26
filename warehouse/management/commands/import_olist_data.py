"""
Import Olist Brazilian E-Commerce CSV data into Django database.

Usage:
    python manage.py import_olist_data /path/to/csv/folder/

Download dataset from:
    https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Extract the zip and point to the folder containing all CSV files.
"""
import os
import csv
from datetime import datetime
from decimal import Decimal, InvalidOperation
from django.core.management.base import BaseCommand
from django.db import transaction
from warehouse.models import (
    Customer, Seller, Product, ProductCategoryTranslation,
    Geolocation, Order, OrderItem, OrderPayment, OrderReview
)


class Command(BaseCommand):
    help = 'Import Olist e-commerce CSV data into database'

    def add_arguments(self, parser):
        parser.add_argument(
            'csv_folder',
            type=str,
            help='Path to folder containing Olist CSV files'
        )
        parser.add_argument(
            '--skip-geo',
            action='store_true',
            help='Skip geolocation data (1M+ rows, takes time)'
        )

    def handle(self, *args, **options):
        folder = options['csv_folder']

        if not os.path.isdir(folder):
            self.stderr.write(self.style.ERROR(f"❌ Folder not found: {folder}"))
            return

        # Check which CSV files exist
        expected_files = {
            'customers': 'olist_customers_dataset.csv',
            'sellers': 'olist_sellers_dataset.csv',
            'products': 'olist_products_dataset.csv',
            'translation': 'product_category_name_translation.csv',
            'geolocation': 'olist_geolocation_dataset.csv',
            'orders': 'olist_orders_dataset.csv',
            'order_items': 'olist_order_items_dataset.csv',
            'payments': 'olist_order_payments_dataset.csv',
            'reviews': 'olist_order_reviews_dataset.csv',
        }

        self.stdout.write("\n📂 Checking CSV files...")
        found_files = {}
        for key, filename in expected_files.items():
            filepath = os.path.join(folder, filename)
            if os.path.exists(filepath):
                found_files[key] = filepath
                self.stdout.write(f"  ✅ {filename}")
            else:
                self.stdout.write(self.style.WARNING(f"  ⚠️  {filename} — NOT FOUND"))

        if not found_files:
            self.stderr.write(self.style.ERROR("\n❌ No CSV files found! Check folder path."))
            return

        self.stdout.write(f"\n🚀 Starting import ({len(found_files)} files found)...\n")

        # Import in correct order (respecting foreign keys)
        import_order = [
            ('customers', self._import_customers),
            ('sellers', self._import_sellers),
            ('products', self._import_products),
            ('translation', self._import_translations),
            ('orders', self._import_orders),
            ('order_items', self._import_order_items),
            ('payments', self._import_payments),
            ('reviews', self._import_reviews),
        ]

        if not options['skip_geo']:
            import_order.append(('geolocation', self._import_geolocation))
        else:
            self.stdout.write("⏭️  Skipping geolocation (--skip-geo flag)")

        for key, import_func in import_order:
            if key in found_files:
                import_func(found_files[key])

        self.stdout.write(self.style.SUCCESS(
            f"\n{'='*50}\n"
            f"✅ IMPORT COMPLETE!\n"
            f"{'='*50}\n"
            f"  Customers:    {Customer.objects.count():,}\n"
            f"  Sellers:      {Seller.objects.count():,}\n"
            f"  Products:     {Product.objects.count():,}\n"
            f"  Translations: {ProductCategoryTranslation.objects.count():,}\n"
            f"  Orders:       {Order.objects.count():,}\n"
            f"  Order Items:  {OrderItem.objects.count():,}\n"
            f"  Payments:     {OrderPayment.objects.count():,}\n"
            f"  Reviews:      {OrderReview.objects.count():,}\n"
            f"  Geolocation:  {Geolocation.objects.count():,}\n"
        ))

    # ========================================
    # Helper methods
    # ========================================
    def _read_csv(self, filepath):
        """Read CSV file and return rows as dicts."""
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return list(reader)

    def _parse_datetime(self, value):
        """Parse datetime string, return None if empty/invalid."""
        if not value or value.strip() == '':
            return None
        try:
            # Try common formats
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S']:
                try:
                    return datetime.strptime(value.strip(), fmt)
                except ValueError:
                    continue
            return None
        except Exception:
            return None

    def _parse_int(self, value):
        """Parse integer, return None if empty/invalid."""
        if not value or value.strip() == '':
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _parse_decimal(self, value):
        """Parse decimal, return 0 if empty/invalid."""
        if not value or value.strip() == '':
            return Decimal('0')
        try:
            return Decimal(value.strip())
        except (InvalidOperation, ValueError):
            return Decimal('0')

    # ========================================
    # Import functions for each table
    # ========================================
    @transaction.atomic
    def _import_customers(self, filepath):
        self.stdout.write("👥 Importing customers...")
        rows = self._read_csv(filepath)
        Customer.objects.all().delete()

        batch = []
        for row in rows:
            batch.append(Customer(
                customer_id=row['customer_id'],
                customer_unique_id=row['customer_unique_id'],
                customer_zip_code_prefix=row.get('customer_zip_code_prefix', ''),
                customer_city=row.get('customer_city', ''),
                customer_state=row.get('customer_state', ''),
            ))

        Customer.objects.bulk_create(batch, batch_size=5000)
        self.stdout.write(self.style.SUCCESS(f"   ✅ {len(batch):,} customers imported"))

    @transaction.atomic
    def _import_sellers(self, filepath):
        self.stdout.write("🏪 Importing sellers...")
        rows = self._read_csv(filepath)
        Seller.objects.all().delete()

        batch = []
        for row in rows:
            batch.append(Seller(
                seller_id=row['seller_id'],
                seller_zip_code_prefix=row.get('seller_zip_code_prefix', ''),
                seller_city=row.get('seller_city', ''),
                seller_state=row.get('seller_state', ''),
            ))

        Seller.objects.bulk_create(batch, batch_size=5000)
        self.stdout.write(self.style.SUCCESS(f"   ✅ {len(batch):,} sellers imported"))

    @transaction.atomic
    def _import_products(self, filepath):
        self.stdout.write("📦 Importing products...")
        rows = self._read_csv(filepath)
        Product.objects.all().delete()

        batch = []
        for row in rows:
            batch.append(Product(
                product_id=row['product_id'],
                product_category_name=row.get('product_category_name', '') or None,
                product_name_length=self._parse_int(row.get('product_name_lenght', '')),
                product_description_length=self._parse_int(row.get('product_description_lenght', '')),
                product_photos_qty=self._parse_int(row.get('product_photos_qty', '')),
                product_weight_g=self._parse_int(row.get('product_weight_g', '')),
                product_length_cm=self._parse_int(row.get('product_length_cm', '')),
                product_height_cm=self._parse_int(row.get('product_height_cm', '')),
                product_width_cm=self._parse_int(row.get('product_width_cm', '')),
            ))

        Product.objects.bulk_create(batch, batch_size=5000)
        self.stdout.write(self.style.SUCCESS(f"   ✅ {len(batch):,} products imported"))

    @transaction.atomic
    def _import_translations(self, filepath):
        self.stdout.write("🌐 Importing category translations...")
        rows = self._read_csv(filepath)
        ProductCategoryTranslation.objects.all().delete()

        batch = []
        for row in rows:
            cat_name = row.get('product_category_name', '').strip()
            eng_name = row.get('product_category_name_english', '').strip()
            if cat_name and eng_name:
                batch.append(ProductCategoryTranslation(
                    product_category_name=cat_name,
                    product_category_name_english=eng_name,
                ))

        ProductCategoryTranslation.objects.bulk_create(batch, batch_size=1000)
        self.stdout.write(self.style.SUCCESS(f"   ✅ {len(batch):,} translations imported"))

    @transaction.atomic
    def _import_orders(self, filepath):
        self.stdout.write("🛒 Importing orders...")
        rows = self._read_csv(filepath)
        Order.objects.all().delete()

        # Get valid customer IDs
        valid_customers = set(Customer.objects.values_list('customer_id', flat=True))

        batch = []
        skipped = 0
        for row in rows:
            cust_id = row['customer_id']
            if cust_id not in valid_customers:
                skipped += 1
                continue

            batch.append(Order(
                order_id=row['order_id'],
                customer_id=cust_id,
                order_status=row.get('order_status', ''),
                order_purchase_timestamp=self._parse_datetime(row.get('order_purchase_timestamp', '')),
                order_approved_at=self._parse_datetime(row.get('order_approved_at', '')),
                order_delivered_carrier_date=self._parse_datetime(row.get('order_delivered_carrier_date', '')),
                order_delivered_customer_date=self._parse_datetime(row.get('order_delivered_customer_date', '')),
                order_estimated_delivery_date=self._parse_datetime(row.get('order_estimated_delivery_date', '')),
            ))

        Order.objects.bulk_create(batch, batch_size=5000)
        self.stdout.write(self.style.SUCCESS(f"   ✅ {len(batch):,} orders imported (skipped {skipped})"))

    @transaction.atomic
    def _import_order_items(self, filepath):
        self.stdout.write("📋 Importing order items...")
        rows = self._read_csv(filepath)
        OrderItem.objects.all().delete()

        valid_orders = set(Order.objects.values_list('order_id', flat=True))
        valid_products = set(Product.objects.values_list('product_id', flat=True))
        valid_sellers = set(Seller.objects.values_list('seller_id', flat=True))

        batch = []
        skipped = 0
        for row in rows:
            oid = row['order_id']
            pid = row['product_id']
            sid = row['seller_id']

            if oid not in valid_orders or pid not in valid_products or sid not in valid_sellers:
                skipped += 1
                continue

            batch.append(OrderItem(
                order_id=oid,
                order_item_id=int(row.get('order_item_id', 1)),
                product_id=pid,
                seller_id=sid,
                shipping_limit_date=self._parse_datetime(row.get('shipping_limit_date', '')),
                price=self._parse_decimal(row.get('price', '0')),
                freight_value=self._parse_decimal(row.get('freight_value', '0')),
            ))

            # Bulk create in chunks to avoid memory issues
            if len(batch) >= 10000:
                OrderItem.objects.bulk_create(batch, batch_size=5000)
                self.stdout.write(f"   ... {OrderItem.objects.count():,} items so far")
                batch = []

        if batch:
            OrderItem.objects.bulk_create(batch, batch_size=5000)

        self.stdout.write(self.style.SUCCESS(
            f"   ✅ {OrderItem.objects.count():,} order items imported (skipped {skipped})"
        ))

    @transaction.atomic
    def _import_payments(self, filepath):
        self.stdout.write("💳 Importing payments...")
        rows = self._read_csv(filepath)
        OrderPayment.objects.all().delete()

        valid_orders = set(Order.objects.values_list('order_id', flat=True))

        batch = []
        skipped = 0
        for row in rows:
            oid = row['order_id']
            if oid not in valid_orders:
                skipped += 1
                continue

            batch.append(OrderPayment(
                order_id=oid,
                payment_sequential=int(row.get('payment_sequential', 1)),
                payment_type=row.get('payment_type', ''),
                payment_installments=int(row.get('payment_installments', 1)),
                payment_value=self._parse_decimal(row.get('payment_value', '0')),
            ))

            if len(batch) >= 10000:
                OrderPayment.objects.bulk_create(batch, batch_size=5000)
                batch = []

        if batch:
            OrderPayment.objects.bulk_create(batch, batch_size=5000)

        self.stdout.write(self.style.SUCCESS(
            f"   ✅ {OrderPayment.objects.count():,} payments imported (skipped {skipped})"
        ))

    @transaction.atomic
    def _import_reviews(self, filepath):
        self.stdout.write("⭐ Importing reviews...")
        rows = self._read_csv(filepath)
        OrderReview.objects.all().delete()

        valid_orders = set(Order.objects.values_list('order_id', flat=True))

        batch = []
        skipped = 0
        seen = set()  # avoid duplicate review_id + order_id combos

        for row in rows:
            oid = row['order_id']
            rid = row.get('review_id', '')

            if oid not in valid_orders:
                skipped += 1
                continue

            combo = f"{rid}_{oid}"
            if combo in seen:
                skipped += 1
                continue
            seen.add(combo)

            batch.append(OrderReview(
                review_id=rid,
                order_id=oid,
                review_score=int(row.get('review_score', 3)),
                review_comment_title=row.get('review_comment_title', '') or None,
                review_comment_message=row.get('review_comment_message', '') or None,
                review_creation_date=self._parse_datetime(row.get('review_creation_date', '')),
                review_answer_timestamp=self._parse_datetime(row.get('review_answer_timestamp', '')),
            ))

            if len(batch) >= 10000:
                OrderReview.objects.bulk_create(batch, batch_size=5000)
                batch = []

        if batch:
            OrderReview.objects.bulk_create(batch, batch_size=5000)

        self.stdout.write(self.style.SUCCESS(
            f"   ✅ {OrderReview.objects.count():,} reviews imported (skipped {skipped})"
        ))

    def _import_geolocation(self, filepath):
        self.stdout.write("🗺️  Importing geolocation (this may take a minute)...")
        rows = self._read_csv(filepath)
        Geolocation.objects.all().delete()

        batch = []
        for row in rows:
            try:
                batch.append(Geolocation(
                    geolocation_zip_code_prefix=row.get('geolocation_zip_code_prefix', ''),
                    geolocation_lat=float(row.get('geolocation_lat', 0)),
                    geolocation_lng=float(row.get('geolocation_lng', 0)),
                    geolocation_city=row.get('geolocation_city', ''),
                    geolocation_state=row.get('geolocation_state', ''),
                ))
            except (ValueError, TypeError):
                continue

            if len(batch) >= 20000:
                with transaction.atomic():
                    Geolocation.objects.bulk_create(batch, batch_size=10000)
                self.stdout.write(f"   ... {Geolocation.objects.count():,} locations so far")
                batch = []

        if batch:
            with transaction.atomic():
                Geolocation.objects.bulk_create(batch, batch_size=10000)

        self.stdout.write(self.style.SUCCESS(
            f"   ✅ {Geolocation.objects.count():,} geolocation records imported"
        ))
