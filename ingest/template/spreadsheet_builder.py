import xlsxwriter

from ingest.template.tab_config import TabConfig
from .schema_template import SchemaTemplate

DEFAULT_INGEST_URL = "http://api.ingest.data.humancellatlas.org"
DEFAULT_SCHEMAS_ENDPOINT = "/schemas/search/latestSchemas"
DEFAULT_MIGRATIONS_URL = "https://schema.humancellatlas.org/property_migrations"
MIN_WIDTH_OF_COLUMN = 25


class SpreadsheetBuilder():

    def build(self, schema_template):
        """ Builds an empty spreadsheet. """
        raise NotImplementedError("Subclasses of SpreadsheetBuilder are required to override this method.")

    def create_initial_spreadsheet(self, output_file, hide_row):
        """
        Creates an initial empty spreadsheet with a set formatting.

        :param output_file: A string representing the name of the Excel file that will be generated by this class.
        :param hide_row: A boolean where when true, hides the third row in the generated spreadsheet which contains
                         the fully qualified name of the metadata schema field.
        """

        self.spreadsheet = xlsxwriter.Workbook(output_file)
        self.header_format = self.spreadsheet.add_format(
            {'bold': True, 'bg_color': '#D0D0D0', 'font_size': 12, 'valign': 'vcenter'})
        self.locked_format = self.spreadsheet.add_format({'locked': True})
        self.desc_format = self.spreadsheet.add_format(
            {'font_color': '#808080', 'italic': True, 'text_wrap': True, 'font_size': 12, 'valign': 'top'})
        self.include_schemas_tab = False
        self.hidden_row = hide_row

    def save_spreadsheet(self):
        self.spreadsheet.close()

    def generate_spreadsheet(self, schema_urls=None, tabs_template=None, include_schemas_tab=False,
                             schema_template=None):
        """
        Given a template that represents the tabs configuration in the desired spreadsheet and a metadata schema,
        generates the respective spreadsheet. If include_schema_tab is set to True, the spreadsheet will have an
        additional sheet containing the URLs of the metadata schemas that were used to generate the spreadsheet.

        :param schema_urls: A list of strings where each string represents a URL containing a JSON-formatted metadata
                            schema.
        :param tabs_template: A string representing a YAML file that contains a configuration specifying how the tabs in
                              the generated spreadsheet should look and what information/columns it should contain.
        :param include_schemas_tab: A boolean, when true, includes an additional sheet at the end of the spreadsheet
                                    that contains a list of metadata schemas that were used to create the spreadsheet.
        :param schema_template: A SchemaTemplate object that has already been created that can directly be used to
                                generate the spreadsheet.
        """

        self.include_schemas_tab = include_schemas_tab
        if tabs_template and schema_urls:
            tabs_parser = TabConfig()
            tabs = tabs_parser.load(tabs_template)
            template = SchemaTemplate(metadata_schema_urls=schema_urls, tab_config=tabs)
        elif schema_urls:
            template = SchemaTemplate(metadata_schema_urls=schema_urls, migrations_url=DEFAULT_MIGRATIONS_URL)
        elif schema_template:
            template = schema_template
        else:
            template = SchemaTemplate()

        self.build(template)

    def get_value_for_column(self, template, column_name, property):
        """ Lookup the value of a column name and property in a SchemaTemplate. `lookup` throws an exception if no
        property exists so capture the exception and return an empty string if no property exists for the given column
        and template.
        :param template: a schema template object
        :param column_name: the full programmatic name of the column
        :param property: the property you want to retrieve eg. example, description
        """
        try:
            if ".text" in column_name:
                value = template.lookup_property_attributes_in_metadata(column_name.replace('.text', '') +
                                                                        "." + property)
                return str(value) if value else ""
            else:
                value = template.lookup_property_attributes_in_metadata(column_name + "." + property)
            return str(value) if value else ""
        except Exception:
            try:
                value = template.lookup_property_attributes_in_metadata(
                    column_name.replace('.text', '') + "." + property)
                return str(value) if value else ""
            except Exception:
                print("No property " + property + " for " + column_name)
                return ""

    def get_user_friendly_column_name(self, template, column_name, primary_schema=None):
        # TODO(maniarathi): Make this description better.
        """ Given a column name derived originally from a metadata schema file that will be inputted as a column name
        into the generated spreadsheet, turn it into a user friendly name. """

        if '.text' in column_name:
            key = column_name.replace('.text', '') + ".user_friendly"
        elif '.ontology_label' in column_name:
            key = column_name.replace('.ontology_label', '') + ".user_friendly"
        elif '.ontology' in column_name:
            key = column_name.replace('.ontology', '') + ".user_friendly"
        else:
            key = column_name + ".user_friendly"

        try:
            try:
                uf = template.lookup_property_attributes_in_metadata(key)
            except Exception:
                uf = template.lookup_property_attributes_in_metadata(column_name + ".user_friendly")

            wrapper = ".".join(column_name.split(".")[:-1])
            if template.lookup_property_attributes_in_metadata(wrapper)['schema']['module'] == 'purchased_reagents' \
                    and not template.lookup_property_attributes_in_metadata(wrapper)['multivalue']:
                uf = template.lookup_property_attributes_in_metadata(wrapper)['user_friendly'] + " - " + uf

            if template.lookup_property_attributes_in_metadata(wrapper)['schema']['module'] == 'barcode'\
                    and not template.lookup_property_attributes_in_metadata(wrapper)['multivalue']:
                uf = template.lookup_property_attributes_in_metadata(wrapper)['user_friendly'] + " - " + uf

            # Add exception for modules that add modules e.g. "cell_suspension.cell_morphology.cell_size_unit.text"
            if len(column_name.split(".")) > 3:
                split_column_name = column_name.split(".")
                module_path = ".".join(split_column_name[0:2])
                module_schema_url = template.lookup_property_attributes_in_metadata(module_path)['schema']['url']
                module_template = SchemaTemplate(metadata_schema_urls=[module_schema_url])
                uf = module_template.meta_data_properties[split_column_name[1]][split_column_name[2]]['user_friendly']

            if '.ontology_label' in column_name and 'ontology label' not in uf:
                uf = uf + " ontology label"
            elif '.ontology' in column_name and 'ontology' not in uf:
                uf = uf + " ontology ID"

            if "Biomaterial " in uf:
                schema_name = column_name.split(".")[0]

                for schema in template.tabs:
                    if schema_name == list(schema.keys())[0]:
                        schema_uf = schema[schema_name]['display_name']
                uf = uf.replace("Biomaterial", schema_uf)

                if primary_schema != schema_name:
                    uf = "Input " + uf

            if "Protocol " in uf:
                schema_name = column_name.split(".")[0]

                for schema in template.tabs:
                    if schema_name == list(schema.keys())[0]:
                        schema_uf = schema[schema_name]['display_name']
                uf = uf.replace("Protocol", schema_uf)

            return uf
        except Exception as e:
            print(e)
            try:
                return uf
            except:
                return column_name

    def generate_and_add_schema_worksheet_to_spreadsheet(self, schema_urls):
        worksheet = self.spreadsheet.add_worksheet("Schemas")
        worksheet.write(0, 0, "Schemas")
        for index, url in enumerate(schema_urls):
            worksheet.write(index + 1, 0, url)
