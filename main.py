from typing import AsyncGenerator
from uuid import uuid4

import aiohttp
from aiohttp import web
from fhirpy import AsyncFHIRClient

REPOSITORY_BASE_URL = "https://erequesting.aidbox.beda.software/fhir"
EMR_BASE_URL = "http://localhost:8080/fhir"


def identifier(order_id):
    value = "BEDA0325-%06d" % (order_id)
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
        "system": "http://diagnostic-orders-are-us.com.au/ids/pgn",
        "value": value,
    }


def contained(patient_id):
    return [
        {
            "resourceType": "Coverage",
            "id": "coverage",
            "status": "active",
            "type": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                        "code": "PUBLICPOL",
                        "display": "public healthcare",
                    }
                ],
                "text": "Bulk Billed",
            },
            "beneficiary": {
                "reference": f"urn:uuid:{patient_id}",
            },
            "payor": [{"type": "Organization", "display": "Medicare Australia"}],
        },
        {
            "resourceType": "Encounter",
            "id": "encounter",
            "status": "finished",
            "class": {
                "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                "code": "AMB",
                "display": "ambulatory",
            },
        },
    ]


def clean_meta(data):
    profile = data.get("meta", {}).get("profile")
    del data["meta"]
    if profile:
        data["meta"] = {"profile": profile}
    return data


async def prepare_service_request(sr, order_number):
    organization = await sr["performer"][0].to_resource()
    remote_org_id = organization["identifier"][0]["value"]
    remote_organization = {
        "reference": f"Organization/{remote_org_id}",
    }

    requester = await sr["requester"].to_resource()
    remote_requester_id = requester["identifier"][0]["value"]
    remote_requester = {
        "reference": f"PractitionerRole/{remote_requester_id}",
    }

    patient = await sr["subject"].to_resource()
    patient_data = patient.serialize()
    clean_meta(patient_data)
    patient_id = patient_data["id"]
    del patient_data["id"]

    encounter = await sr["encounter"].to_resource()
    encounter_data = encounter.serialize()
    encounter_id = encounter_data["id"]

    clean_meta(encounter_data)
    del encounter_data["id"]
    del encounter_data["participant"]
    del encounter_data["class"]
    encounter_data["class"] = {
        "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
        "code": "AMB",
    }
    encounter_data["subject"] = {"reference": f"urn:uuid:{patient_id}"}

    sr_id = uuid4()
    group_task_id = uuid4()
    task_id = uuid4()

    external_sr = {
        "resourceType": "ServiceRequest",
        "meta": {
            "profile": [
                (
                    "http://hl7.org.au/fhir/ereq/StructureDefinition/au-erequesting-servicerequest-path"
                    if sr["category"][0]["coding"][0]["code"] == "108252007"
                    else "http://hl7.org.au/fhir/ereq/StructureDefinition/au-erequesting-servicerequest-imag"
                )
            ],
        },
        "requisition": identifier(order_number),
        "id": str(sr_id),
        "contained": contained(patient_id),
        "authoredOn": "2024-12-12T10:00:00+10:00",
        "category": sr["category"],
        "code": sr["code"],
        "priority": sr["priority"],
        "requester": remote_requester,
        "status": "active",
        "intent": sr["intent"],
        "subject": {"reference": f"urn:uuid:{patient_id}"},
        "encounter": {"reference": "#encounter"},
        "insurance": [{"reference": "#coverage"}],
        "extension": [
            {
                "url": "http://hl7.org.au/fhir/ereq/StructureDefinition/au-erequesting-displaysequence",
                "valueInteger": 1,
            }
        ],
    }
    external_group_task = {
        "resourceType": "Task",
        "meta": {
            "profile": [
                "http://hl7.org.au/fhir/ereq/StructureDefinition/au-erequesting-task"
            ],
            "tag": [
                {
                    "system": "http://hl7.org.au/fhir/ereq/CodeSystem/au-erequesting-task-tag",
                    "code": "fulfilment-task-group",
                }
            ],
        },
        "groupIdentifier": identifier(order_number),
        "status": "requested",
        "businessStatus": {
            "coding": [
                {
                    "system": "http://sonichealthcare.com.au/CodeSystem/pathology-order-status",
                    "code": "active",
                }
            ]
        },
        "priority": sr["priority"],
        "code": {
            "coding": [
                {
                    "system": "http://hl7.org/fhir/CodeSystem/task-code",
                    "code": "fulfill",
                }
            ]
        },
        "intent": "order",
        "focus": {"reference": f"urn:uuid:{str(sr_id)}"},
        "owner": remote_organization,
        "authoredOn": "2025-03-19T10:00:00+10:00",
        "for": {"reference": f"urn:uuid:{patient_id}"},
        "requester": remote_requester,
    }

    external_task = {
        "resourceType": "Task",
        "meta": {
            "profile": [
                "http://hl7.org.au/fhir/ereq/StructureDefinition/au-erequesting-task"
            ],
            "tag": [
                {
                    "system": "http://hl7.org.au/fhir/ereq/CodeSystem/au-erequesting-task-tag",
                    "code": "fulfilment-task",
                }
            ],
        },
        "groupIdentifier": identifier(order_number),
        "status": "requested",
        "businessStatus": {
            "coding": [
                {
                    "system": "http://sonichealthcare.com.au/CodeSystem/pathology-order-status",
                    "code": "active",
                }
            ]
        },
        "priority": sr["priority"],
        "code": {
            "coding": [
                {
                    "system": "http://hl7.org/fhir/CodeSystem/task-code",
                    "code": "fulfill",
                }
            ]
        },
        "intent": "order",
        "focus": {"reference": f"urn:uuid:{str(sr_id)}"},
        "owner": remote_organization,
        "authoredOn": "2024-03-21T10:00:00+10:00",
        "for": {"reference": f"urn:uuid:{patient_id}"},
        "requester": remote_requester,
    }

    return {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": [
            {
                "request": {"url": "ServiceRequest", "method": "POST"},
                "resource": external_sr,
                "fullUrl": f"urn:uuid:{str(sr_id)}",
            },
            {
                "request": {"url": "Patient", "method": "POST"},
                "resource": patient_data,
                "fullUrl": f"urn:uuid:{str(patient_id)}",
            },
            {
                "request": {"url": "Task", "method": "POST"},
                "resource": external_group_task,
                "fullUrl": f"urn:uuid:{str(group_task_id)}",
            },
            {
                "request": {"url": "Task", "method": "POST"},
                "resource": external_task,
                "fullUrl": f"urn:uuid:{str(task_id)}",
            },
            {
                "request": {"url": "Encounter", "method": "POST"},
                "resource": encounter_data,
                "fullUrl": f"urn:uuid:{str(encounter_id)}",
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
    order_number = await emr.resources("ServiceRequest").count()
    bundle = repository.resource(
        "Bundle", **(await prepare_service_request(sr, order_number))
    )
    # import json
    # print(json.dumps(bundle.serialize(), indent=2))
    await bundle.save()

    print(bundle.serialize())

    location = bundle["entry"][0]["response"]["location"]
    if "pyroserver.azurewebsites.net" in location:
        external_sr_id = location.split("/")[5]
    else:
        external_sr_id = location.split("/")[1]

    identifiers = sr.get("identifier", [])
    identifiers.append({"system": system, "value": external_sr_id})
    sr["identifier"] = identifiers
    await sr.save(fields=["identifier"])

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
        REPOSITORY_BASE_URL,
        # authorization="Basic cGxhY2VyOnBzOHFzN2tMVmJqUzVHcg=="
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
