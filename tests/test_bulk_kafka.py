"""ACK ficticios de prueba: nunca se conectan a Aiven ni acreditan ingesta."""
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from bulk_lakehouse.kafka_async import publish


class AsyncProducerTests(unittest.TestCase):
    def test_bounded_pending_durable_resume_and_stable_identity(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            parts=root/"parts"
            parts.mkdir()
            m={"schema_version":"twitch-bulk-parquet-v1","source_sha256":"a"*64,
                "requested_rows":100000,"extracted_rows":100000,
                "parts":[{"file":"part-000000.parquet","first_row":1,"last_row":100000,"rows":100000,"bytes":1,"sha256":"b"*64}]}
            (parts/"manifest.json").write_text(json.dumps(m))
            review={"status":"APPROVED","rows":100000,"source_sha256":"a"*64,
                "plan_change_allowed":False,"additional_charges_allowed":False,"credits_and_storage_checked":True,
                "recovery_reviewed":True,"topic":"twitch.bulk.fixture","partitions":[0,1],"max_message_bytes":1000}
            sample=[{"source_row":i,"raw_line":"1,1,fixture,0,2","raw_sha256":"c"*64} for i in range(1,129)]
            class Producer:
                def __init__(self): self.pending=0;self.peak=0;self.events=[]
                def partitions_for(self,_): return {0,1}
                def send(self,topic,key,value):
                    self.pending+=1
                    self.peak=max(self.peak,self.pending)
                    self.events.append(value)
                    offset=len(self.events)-1
                    def ack(timeout):
                        self.pending-=1
                        return SimpleNamespace(topic=topic,partition=0,offset=offset)
                    return SimpleNamespace(get=ack)
                def flush(self,**_): pass
                def close(self,**_): pass
            first=Producer()
            with patch("bulk_lakehouse.kafka_async.rows",return_value=iter(sample)),patch("bulk_lakehouse.kafka_async.time.sleep"):
                result=publish(parts,root/"state",review,lambda:first,max_pending=32)
            self.assertEqual(first.peak,32)
            self.assertEqual(result["acknowledged_distinct_source_rows"],128)
            self.assertEqual(result["status"],"STOPPED")  # Nunca inventar los otros 99872
            second=Producer()
            with patch("bulk_lakehouse.kafka_async.rows",return_value=iter(sample)),patch("bulk_lakehouse.kafka_async.time.sleep"):
                resumed=publish(parts,root/"state",review,lambda:second,max_pending=32)
            self.assertEqual(len(second.events),0)
            self.assertEqual(resumed["load_id"],result["load_id"])
            with patch("bulk_lakehouse.kafka_async.rows",return_value=iter(sample)),self.assertRaises(ValueError):
                publish(parts,root/"other",{**review,"topic":"twitch.pilot.twitch1000"},lambda:self.fail("No conectar"))


if __name__=="__main__": unittest.main()
