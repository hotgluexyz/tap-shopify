class CompatibilityMixin:
    @staticmethod
    def _extract_int_id(string_id):
        """
        Extract the integer ID from a Shopify global ID string.
        Example input: "gid://shopify/Product/8156477030620"
        Example output: 8156477030620
        """
        return int(string_id.split("/")[-1])

    @staticmethod
    def _cast_values(data, mappings):
        """
        Recursively traverse and cast values in a dictionary or list based on mappings.
        :param data: The data to process (dictionary, list, or scalar).
        :param mappings: The mapping dictionary to use for casting.
        :return: The processed data with values cast according to the mappings.
        """
        if isinstance(data, dict):
            return {
                key: CompatibilityMixin._cast_values(value, mappings.get(key, {}))
                for key, value in data.items()
            }
        elif isinstance(data, list):
            return [CompatibilityMixin._cast_values(item, mappings) for item in data]
        elif data in mappings:
            return mappings[data]
        return data
