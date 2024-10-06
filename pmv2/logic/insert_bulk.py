"""Part of insert-services-bulk logic is located here."""

from pydantic import BaseModel

from pmv2.urban_client.models import PhysicalObjectType, ServiceType

class UploadFileConfig(BaseModel):
    """Single file configuration."""

    service_type: str
    physical_object_type: str


class UploadConfig(BaseModel):
    """Configuration for uploading geojson files as service GeoDataFrames."""

    filenames: dict[str, UploadFileConfig]

    def transform_to_ids(
        self, service_types: list[ServiceType], physical_object_types: list[PhysicalObjectType]
    ) -> "UploadConfigWithIDs":
        """Transform to validated upload config with names replaced by identifiers."""
        missing_service_types = set(file.service_type for file in self.filenames.values()) - set(
            st.name for st in service_types
        )
        missing_physical_object_types = set(file.physical_object_type for file in self.filenames.values()) - set(
            pot.name for pot in physical_object_types
        )
        errors = []
        if len(missing_service_types) > 0:
            errors.append(f"missing service_types: {', '.join(sorted(missing_service_types))}")
        if len(missing_physical_object_types) > 0:
            errors.append(f"missing physical_object_types: {', '.join(sorted(missing_physical_object_types))}")
        if len(errors) > 0:
            raise ValueError("; ".join(errors))

        st_mapping = {st.name: st.service_type_id for st in service_types}
        pot_mapping = {pot.name: pot.physical_object_type_id for pot in physical_object_types}

        return UploadConfigWithIDs(
            filenames={
                filename: UploadFileConfigWithIDs(
                    service_type_id=st_mapping[values.service_type],
                    physical_object_type_id=pot_mapping[values.physical_object_type],
                )
                for filename, values in self.filenames.items()
            }
        )

class UploadFileConfigWithIDs(BaseModel):
    """Single file configuration."""

    service_type_id: int
    physical_object_type_id: int


class UploadConfigWithIDs(BaseModel):
    """Inner view on upload config with names replaced by identifiers and validated."""

    filenames: dict[str, UploadFileConfigWithIDs]
