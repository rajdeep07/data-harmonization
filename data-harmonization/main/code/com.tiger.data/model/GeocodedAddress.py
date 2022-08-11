from typing import Optional
import com.tiger.data.model.PostalAddress

class GeocodedAddress:
    def __init__(self, address, city, state, country, zipCode,
                 latitude = Optional[str],
                 longitude = Optional[str]):
        self.address = address
        self.city = city
        self.state = state
        self.country = country
        self.zipCode = zipCode
        self.latitude = latitude
        self.longitude = longitude

    def get_geo_address(self, PostalAddress):

        internal_state = GeocodedAddress(
            address = PostalAddress.address,
            city = PostalAddress.city,
            state = PostalAddress.state,
            country = PostalAddress.country,
            zipCode = PostalAddress.zipCode,
    )

        return internal_state