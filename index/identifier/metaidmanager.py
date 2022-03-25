from index.identifier.identifiermanager import IdentifierManager
from re import sub, match
from urllib.parse import unquote, quote
from requests import get
from json import loads
from index.storer.csvmanager import CSVManager
from requests import ReadTimeout
from requests.exceptions import ConnectionError
from time import sleep

class MetaIDManager(IdentifierManager):
    def __init__(self, valid_metaid=None, use_api_service=True):
        if valid_metaid is None:
            valid_metaid = CSVManager(store_new=False)

        self.valid_metaid = valid_metaid
        self.p = "br\\"
        self.use_api_service=use_api_service
        self.metaid_uri = "https://w3id.org/oc/meta/br/060"
        super(MetaIDManager, self).__init__()

    def set_valid(self, id_string):
        metaid = self.normalise(id_string, include_prefix=True)

        if self.valid_metaid.get_value(metaid) is None:
            self.valid_metaid.add_value(metaid, "v")

    def is_valid(self, id_string):
        metaid = self.normalise(id_string, include_prefix=True)

        if metaid is None or match("^br\\10\\..+/.+$", metaid) is None:
            return False
        else:
            if self.valid_metaid.get_value(metaid) is None:
                if self.__metaid_exists(metaid):
                    self.valid_metaid.add_value(metaid, "v")
                else:
                    self.valid_metaid.add_value(metaid, "i")

            return "v" in self.valid_metaid.get_value(metaid)

    def normalise(self, id_string, include_prefix=False):
        try:
            metaid_string = sub("\0+", "", sub("\s+", "", unquote(id_string[id_string.index("060"):])))
            return "%s%s" % (self.p if include_prefix else "", metaid_string.lower().strip())
        except:  # Any error in processing the MetaID will return None
            return None

    def __metaid_exists(self, metaid_full):
        if self.use_api_service:
            metaid = self.normalise(metaid_full)
            tentative = 3
            while tentative:
                tentative -= 1
                try:
                    r = get(self.api + quote(metaid), headers=self.headers, timeout=30)
                    if r.status_code == 200:
                        r.encoding = "utf-8"
                        json_res = loads(r.text)
                        return json_res.get("responseCode") == 1
                except ReadTimeout:
                    pass  # Do nothing, just try again
                except ConnectionError:
                    sleep(5)  # Sleep 5 seconds, then try again

        return False
