from typing import AsyncGenerator
from uuid import uuid4

import aiohttp
from aiohttp import web
from fhirpy import AsyncFHIRClient

practitioner_role = "bennett-amanda"
import logging

REPOSITORY_BASE_URL = "https://sparked.npd.telstrahealth.com/ereq/fhir"
EMR_BASE_URL = "http://localhost:8080/fhir"


def identifier(id):
    return {
        "type": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                    "code": "PGN",
                }
            ],
            "text": "Placer Group Number",
        },
        "system": "https://emr.beda.software/ServiceReqeust",
        "value": id,
    }


async def prepare_service_request(sr):
    patient = await sr["subject"].to_resource()
    patient_data = patient.serialize()
    del patient_data["meta"]

    sr_id = uuid4()
    task_id = uuid4()
    external_sr = {
        "resourceType": "ServiceRequest",
        "requisition": identifier(sr["id"]),
        "id": str(sr_id),
        "contained": [patient_data],
        "authoredOn": "2024-03-21",
        "category": [
            {
                "coding": [
                    {
                        "code": "108252007",
                        "display": "Laboratory procedure",
                        "system": "http://snomed.info/sct",
                    }
                ]
            }
        ],
        "code": sr["code"],
        "priority": sr["priority"],
        "requester": {"reference": f"PractitionerRole/{practitioner_role}"},
        "status": "active",
        "intent": sr["intent"],
        "subject": {"reference": f"#{patient['id']}"},
    }
    external_task = {
        "resourceType": "Task",
        "groupIdentifier": identifier(sr["id"]),
        "status": "requested",
        "intent": "order",
        "focus": {"reference": f"ServiceRequest/{str(sr_id)}"},
        "owner": sr["performer"][0],
    }
    return {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": [
            {
                "request": {"url": "ServiceRequest", "method": "POST"},
                "resource": external_sr,
                "fullUrl": f"ServiceRequest/{str(sr_id)}",
            },
            {
                "request": {"url": "Task", "method": "POST"},
                "resource": external_task,
                "fullUrl": f"Task/{str(task_id)}",
            },
        ],
    }


async def syncronize(request):
    data = await request.json()
    sr_id = data["resource"]["id"]
    emr = request.app["emr"]
    repository = request.app["repository"]

    sr = await emr.resources("ServiceRequest").search(_id=sr_id).first()

    system = f"{REPOSITORY_BASE_URL}/ServiceRequest"
    for i in sr.get("identifier", []):
        if i["system"] == system:
            raise Exception("Already synchronized")
    bundle = repository.resource("Bundle", **(await prepare_service_request(sr)))
    await bundle.save()
    external_sr_id = bundle["entry"][0]["response"]["location"].split("/")[1]
    sr["identifier"] = [{"system": system, "value": external_sr_id}]
    await sr.save(fields=["identifier"])

    print(data)
    return web.Response()


async def attach(app: aiohttp.web_app.Application) -> AsyncGenerator:
    emr = AsyncFHIRClient(EMR_BASE_URL, authorization="Basic cm9vdDpzZWNyZXQ=")
    sub = emr.resource(
        "SubsSubscription",
        id="e-request-publish",
        status="active",
        trigger={"ServiceRequest": {"event": ["create"]}},
        channel={
            "type": "rest-hook",
            "endpoint": "http://host.docker.internal:8081/syncronize",
            "payload": {"content": "id-only"},
        },
    )
    await sub.save()
    app["emr"] = emr
    app["repository"] = AsyncFHIRClient(
        REPOSITORY_BASE_URL, authorization="Basic ZmlsbGVyOlFmYk51Z1czMnRaWDhuTA=="
    )
    yield
    await sub.delete()
    del app["emr"]
    del app["repository"]


def main():
    app = web.Application()
    app.add_routes([web.post("/syncronize", syncronize)])
    app.cleanup_ctx.append(attach)
    web.run_app(app, port=8081)


if __name__ == "__main__":
    main()
