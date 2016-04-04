
//==================================
// From Data Extension Framework:
//----------------------------------
CREATE TABLE data_entity (
	id uuid PRIMARY KEY,
	type text,
	object text
);

CREATE INDEX ON data_entity (type);

CREATE TABLE data_entity_attribute (
	entity_type text,
	attribute_name text,
	attribute_value text,
	entity_id uuid,
	data_entity_object text,
	PRIMARY KEY ((entity_type, attribute_name, attribute_value), entity_id)
);
//==================================
