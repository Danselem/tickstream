# Glue Data Catalog database registering the Iceberg tables (bronze, silver,
# gold, ops) so Athena — and PyIceberg via the Glue catalog — can find them.

resource "aws_glue_catalog_database" "lake" {
  name        = "tickstream"
  description = "tickstream medallion tables (bronze/silver/gold) and ops tables."
}
