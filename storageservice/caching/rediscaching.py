import json
import threading
from datetime import datetime
import redis
import requests
from caching.topics_caching import *
from kafka import KafkaConsumer
from console_logging.console import Console
console=Console()

class Caching:
    """
    This class handles the caching of the respective module
    And always listens for the event changes in kafka.
    If any event is encountered it will update the caching.
    """

    def __init__(
        self,
        api: dict,
        redis_server:dict,
        camera_group: list = None,
        customer: list = None,
        location: list = None,
        subsite: list = None,  # noqa: E501
        zone: list = None,
    ) -> None:  # noqa: E501
        """
        Initialize the caching
        Args:
            api: dict of apis
            customer: list of customer id, by default is None
            location: list of customer id, by default is None
            subsite: list of subsite id, by default is None
            camera_group: list of camera group, by default is None
        """
        pool = redis.ConnectionPool(host=redis_server["host"], port=redis_server["port"], db=0)
        self.r = redis.Redis(connection_pool=pool)
        print("customer", customer)
        print("location", location)
        print("subsite", subsite)
        print("zone", subsite)
        print("camera_group", camera_group)
        self.customer = customer
        self.camera_group = camera_group
        self.location = location
        self.subsite = subsite
        self.zone = zone
        self.api = api
        self.camera_id=None
        self.topic = PersistTopic(self.api["kafka_by_camid"])

        # self.urllist=urllist

    def getCamId(
        self,
        customer: list = None,
        location: list = None,
        subsite: list = None,
        zone: list = None,
        camera_group: list = None,  # noqa: E501
    ) -> list:  # noqa: E501
        """
        Get all the camera group based on the params
        Args:
            customer: list of customer id, by default is None
            location: list of customer id, by default is None
            subsite: list of subsite id, by default is None
            zone: list of zone id, by default is None
            camera_group: list of camera group, by default is None
        returns:
            list: All the cameragroup
        """
        camgroup = []
        if (
            customer is None and location is None and subsite is None and camera_group is None and zone is None
        ):  # noqa: E501
            try:
                r = requests.get(self.api["camera_id"], timeout=50)
                try:
                    camgroup = r.json()["data"]
                except Exception as ex:
                    print("Camgroup exception: ", ex)
                    return []
            except Exception as ex:
                return []

        if customer is not None:
            try:
                r = requests.get(self.api["camera_id"], json={"customer_id": customer}, timeout=50)  # noqa: E501
            
                camgroup = r.json()["data"]
                # print("===customer==",camgroup)
            except Exception as ex:
                print("Exceptin while fetching customer ", ex)
                pass
        if location is not None:
            try:
                r = requests.get(self.api["camera_id"], json={"location_id": location}, timeout=50)  # noqa: E501
            
                if len(camgroup) > 0:
                    camgroup = camgroup + r.json()["data"]
                    print("===location==", camgroup)
            except Exception as ex:
                print("location data exception: ", ex)
                pass
        if subsite is not None:
            try:
                r = requests.get(self.api["camera_id"], json={"subsite_id": subsite}, timeout=50)  # noqa: E501
            
                if len(camgroup) > 0:
                    camgroup = camgroup + r.json()["data"]
                    print("===subsite==", camgroup)
            except Exception as ex:
                print("subsite data exception: ", ex)
                pass
        if zone is not None:
            try:
                r = requests.get(self.api["camera_id"], json={"zone_id": zone}, timeout=50)  # noqa: E501
            
                if len(camgroup) > 0:
                    camgroup = camgroup + r.json()["data"]
                    print("===Zone Id==", camgroup)
            except Exception as ex:
                print("Zone data exception: ", ex)
                pass
        if camera_group is not None:
            try:
                r = requests.get(self.api["camera_id"], json={"camera_group_id": camera_group}, timeout=50)  # noqa: E501
            
                if len(camgroup) > 0:
                    camgroup = camgroup + r.json()["data"]
                    print("===Camera group Id==", camgroup)
            except Exception as ex:
                print("Camera group exception: ", ex)
                pass

        return camgroup

    def persistData(self):
        camid_conf=None
        while True:
            try:
                camid_conf = self.getCamId(self.customer, self.location, self.subsite,self.camera_group)
                if len(camid_conf)>0:
                    break
                else:
                    console.error("DbAPi is not up.")
                    time.sleep(20)
                    continue
            except Exception as ex:
                console.error(f"Api Not Up: {ex}")
                time.sleep(20)
                continue
        if self.camera_id is not None and len(camid_conf) > 0:
            self.camera_id = self.camera_id + camid_conf
        elif self.camera_id is None and len(camid_conf) > 0:
            self.camera_id = camid_conf
        elif self.camera_id is None and len(camid_conf) == 0:
            self.camera_id = []
        else:
            self.camera_id = self.getCamId()
        console.info(f"Camera ids:{self.camera_id}")
        print("camera group---->", self.camera_id)
        # persistdata, scheduledata = self.schedule.persistData()
        # self.r.set("scheduling", json.dumps(persistdata))
        preprocessconfig = {}
        topicconfig = {}
        # for dt in scheduledata:
        print("&&&&&&&&&&&&&&&", self.camera_id)

        jsonreq = {"camera_id": self.camera_id}
        topicconfig, _ = self.topic.persistData(jsonreq)

        # if len(topicconfig)==0:
        #     topicconfig=tempschedule
        # else:
        #     topicconfig.update(topicconfig)
        print("len of preprocessconfig")
        print("*****====>", len(preprocessconfig))
        self.r.set("topics", json.dumps(topicconfig))

    def checkEvents(self,kafka,topic):
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka,
            auto_offset_reset="latest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="storage_caching"
            
        )
        console.info("===updating cache====")
        self.persistData()
        console.success(f"Cache Updated for storage at {datetime.utcnow()}")
        for message in consumer:
            if message is None:
                continue
            else:
                self.persistData()
                console.success(f"Cache Updated for storage at {datetime.utcnow()}")
