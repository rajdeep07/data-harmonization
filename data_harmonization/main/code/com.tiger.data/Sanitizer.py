from data_harmonization.main.code.com.tiger.data.transformer.utils import StringSupport
from data_harmonization.main.code.com.tiger.data.transformer import CityTransformer, NameTransformer, PostalAddressTransformer, StateTransformer, ZipTransformer
from data_harmonization.main.code.com.tiger.data.model import GeocodedAddress, PostalAddress, RawProfile, Profile


class Sanitizer(RawProfile, Profile):
    def _get_santisized_profile(self):
        profile = Profile(
            id=RawProfile.id,
            name=standardizeName(RawProfile.name) or "",
            city=standardizeCity(RawProfile.city) or "",
            address=standardizePostalAddress(RawProfile.address) or "",
            state=standardizeState(RawProfile.state) or "",
            zipcode=standardizeZipCode(RawProfile.zipcode) or ""
        )

        return profile


