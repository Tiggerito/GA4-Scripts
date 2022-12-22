BEGIN

# can't complete as do not have the products image URL

DECLARE domain DEFAULT 'websiteadvantage.mybigcommerce.com';

CREATE OR REPLACE TABLE `bigcommerce-export.bigcommerce_export.feed_base` 
AS
SELECT 
  CONCAT('https://', domain, custom_url, '?sku=', variants_sku) AS url, # should encode the sku. should add setCurrencyId=x
  (SELECT custom_field_value FROM `bigcommerce-export.bigcommerce_export.bc_product_custom_fields` AS custom_fields WHERE product.product_id = custom_fields.product_id AND custom_field_name = 'Pattern') AS pattern,
  (SELECT custom_field_value FROM `bigcommerce-export.bigcommerce_export.bc_product_custom_fields` AS custom_fields WHERE product.product_id = custom_fields.product_id AND custom_field_name = 'Material') AS material,
  (SELECT custom_field_value FROM `bigcommerce-export.bigcommerce_export.bc_product_custom_fields` AS custom_fields WHERE product.product_id = custom_fields.product_id AND custom_field_name = 'Size') AS size,
  (SELECT custom_field_value FROM `bigcommerce-export.bigcommerce_export.bc_product_custom_fields` AS custom_fields WHERE product.product_id = custom_fields.product_id AND custom_field_name = 'Color') AS color,
  IFNULL(variants_price, price) as price,
  IFNULL(variants_sale_price, sale_price) as sale_price,
  IFNULL(variants_retail_price, retail_price) as retail_price,
  IFNULL(variants_cost_price, cost_price) as cost_price,
  IFNULL(variants_fixed_cost_shipping_price, fixed_cost_shipping_price) as fixed_cost_shipping_price,
  IFNULL(variants_is_free_shipping, is_free_shipping) as is_free_shipping,
  IFNULL(variants_weight, weight) as weight,
  IFNULL(variants_width, width) as width,
  IFNULL(variants_height, height) as height,
  IFNULL(variants_depth, depth) as depth,
  product_id,
  variants_id,
  variants_sku,
  variants_cost_price,
  variants_price,
  variants_sale_price,
  variants_retail_price,
  variants_weight,
  variants_width,
  variants_height,
  variants_depth,
  variants_is_free_shipping
  variants_fixed_cost_shipping_price,
  variants_upc,
  variants_inventory_level,
  product_name,
  product_type,
  sku AS product_sku,
  universal_product_code,
  global_trade_item_number,
  manufacturer_part_number,
  brand_name,
  description,
  is_featured,
  condition,
  weight AS product_weight,
  width AS product_width,
  depth AS product_depth,
  height AS product_height,
  price AS product_price,
  cost_price AS product_cost_price,
  retail_price AS product_retail_price,
  sale_price AS product_sale_price,
  fixed_cost_shipping_price AS product_fixed_cost_shipping_price,
  is_free_shipping AS product_is_free_shipping,
  order_quantity_minimum,
  order_quantity_maximum,
  inventory_level AS product_inventory_level,
  base_variant_id
FROM `bigcommerce-export.bigcommerce_export.bc_product_variants` AS variants
LEFT JOIN `bigcommerce-export.bigcommerce_export.bc_product` AS product
USING (product_id)
WHERE variants_purchasing_disabled = false
AND preorder_release_date IS NULL
AND availability = 'available'
AND is_preorder_only = false
AND is_price_hidden = false;

CREATE OR REPLACE TABLE `bigcommerce-export.bigcommerce_export.feed_google` 
AS
SELECT 
  variants_sku AS id, # variants_sku, variants_id
  product_name AS title, # could elaborate on it, brand_name
  description,
  url AS link
  # AS image_link # no image URL :-(
FROM `bigcommerce-export.bigcommerce_export.feed_base`;

END;