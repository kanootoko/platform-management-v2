"""Part of insert-services-bulk logic is located here."""

from pydantic import BaseModel

from pmv2.urban_client.models import PhysicalObjectType


class UploadConfig(BaseModel):
    """Configuration for uploading geojson files as service GeoDataFrames."""

    filenames: dict[str, str]

    def transform_to_ids(self, physical_object_types: list[PhysicalObjectType]) -> "UploadConfigWithID":
        """Transform to validated upload config with names replaced by identifiers."""
        missing_physical_object_types = set(
            physical_object_type for physical_object_type in self.filenames.values()
        ) - set(pot.name for pot in physical_object_types)
        if len(missing_physical_object_types) > 0:
            raise ValueError(f"missing physical_object_types: {', '.join(sorted(missing_physical_object_types))}")

        pot_mapping = {pot.name: pot.physical_object_type_id for pot in physical_object_types}

        return UploadConfigWithID(
            filenames={
                filename: pot_mapping[physical_object_type] for filename, physical_object_type in self.filenames.items()
            }
        )


class UploadConfigWithID(BaseModel):
    """Inner view on upload config with names replaced by identifiers and validated."""

    filenames: dict[str, int]
