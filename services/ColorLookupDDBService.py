import logging

import boto3


class ColorLookupDDBService:
    logger = logging.getLogger(__name__)

    def __init__(self, dynamo_db_config):
        dynamodb = boto3.resource("dynamodb", region_name=dynamo_db_config.region)
        self._table = dynamodb.Table(dynamo_db_config.inventory_color_lookup_table_name)
        self._inventory_color_lookup = {}

    def get_color_lookup(self, inv_item_ky):
        key = str(inv_item_ky)

        self.logger.info(f"Checking inventory item key: {key} dual color lookup")

        if key in self._inventory_color_lookup:
            return self._inventory_color_lookup[key]

        response = self._table.get_item(Key={"INV_ITEM_KY": key})
        item = response.get("Item")

        if not item:
            return None

        dual = item.get("DUAL_COLOUR_ST") == "Y"
        has_fluid_val = item.get("HAS_FLUID_FG", "")
        transparent = (has_fluid_val is True) or (str(has_fluid_val).upper() in ("Y", "TRUE", "T", "1"))

        result = {
            "dual_color": dual,
            "transparent_fluid": transparent,
            "EVEN_INK_COLOR_NM": item.get("EVEN_INK_COLOR_NM", ""),
            "ODD_INK_COLOR_NM": item.get("ODD_INK_COLOR_NM", ""),
        }

        self._inventory_color_lookup[key] = result
        return result