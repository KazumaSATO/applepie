import sys
import argparse
import glob
import dataclasses
import cbor2
import pymysql.cursors
import logging
import json
import typing


def _find_files(pattern):
    """"""
    return sorted(
        [filename for filename in glob.glob(pattern, recursive=True)]
    )


def _deserialize_cbors(filepaths: list[str]):
    """"""
    for filepath in filepaths:
        with open(filepath, "rb") as f:
            yield cbor2.load(f)


def _connect(host, port, userpass, db):
    # Connect to the database
    return pymysql.connect(
        host=host,
        user=userpass,
        port=port,
        password=userpass,
        database=db,
        cursorclass=pymysql.cursors.DictCursor,
    )


def _parse_args(args):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    extract = subparsers.add_parser("extract")
    extract.add_argument("log")
    extract.add_argument("host")
    extract.add_argument("port", type=int)
    extract.add_argument("userpass")
    extract.add_argument("db")
    extract.add_argument("output")

    update = subparsers.add_parser("update")
    update.add_argument("update")
    update.add_argument("host")
    update.add_argument("port", type=int)
    update.add_argument("userpass")
    update.add_argument("db")

    return parser.parse_args(args)


def _get_segments(
    cursor: pymysql.cursors.DictCursor, organization_id, industry_id
) -> list[str]:
    cursor.execute(
        """select cc.external_entry_id
    from crunchbase_initial_company ci
    join disruptor d
    on ci.company_company_id = d.company_company_id
    join disruptor_categories dc
    on d.id = dc.disruptor_id
    join company_category cc
    on dc.categories_id = cc.id
    where ci.organization_id = %s and d.report_id = %s""",
        (organization_id, industry_id),
    )
    return [r["external_entry_id"] for r in cursor.fetchall()]


@dataclasses.dataclass
class Competitor:
    organization_id: str
    sort_order: int


def _get_competitors(
    cursor: pymysql.cursors.DictCursor, industry_id, organization_id
):
    cursor.execute(
        """select ci2.organization_id competitor_organization_id,
    dcc.competitor_companies_order competitor_order
    from disruptor d
    join crunchbase_initial_company ci
    on d.company_company_id = ci.company_company_id
    join disruptor_competitor_companies dcc
    on d.id = dcc.disruptor_id
    join crunchbase_initial_company ci2
    on dcc.competitor_companies_company_id = ci2.company_company_id
    where d.report_id = %s and ci.organization_id = %s
    order by ci2.organization_id, dcc.competitor_companies_order
    """,
        (industry_id, organization_id),
    )
    return [
        Competitor(r["competitor_organization_id"], r["competitor_order"])
        for r in cursor.fetchall()
    ]


@dataclasses.dataclass
class LogRecord:
    organization_id: str
    industry_id: int
    competitor_ids: list[int]
    segment_ids: list[int]


def _decode_cbor(cbor: dict) -> typing.Optional[LogRecord]:
    if "update" in cbor:
        disruption = cbor["update"]
    elif "newDisruption" in cbor:
        disruption = cbor["newDisruption"]
    else:
        return None
    segment_ids = [
        c["companyCategoryId"]
        for c in disruption["industrySegmentIds"]["industrySegmentIds"]
    ]
    competitor_ids = [
        d["companyId"]
        for d in disruption["competitorCompanyIds"]["companyIds"]
    ]
    organization_id = cbor["organizationId"]["organizationId"]
    industry_id = cbor["industryId"]["industryId"]
    return LogRecord(organization_id, industry_id, competitor_ids, segment_ids)


def _extract(connection, options):
    cbors = _find_files(options.log)
    with connection, open(options.output, "w") as f:
        sep = ""
        for cbor in _deserialize_cbors(cbors):
            record = _decode_cbor(cbor)
            if record is None:
                logging.error(f"Unknown format: {cbor}")
                sys.exit(1)

            with connection.cursor() as cursor:
                segments: list[str] = _get_segments(
                    cursor, record.organization_id, record.industry_id
                )
            with connection.cursor() as cursor:
                competitors: list[Competitor] = _get_competitors(
                    cursor, record.industry_id, record.organization_id
                )
            line = {
                "organization_id": record.organization_id,
                "industry_id": record.industry_id,
                "segments": segments,
                "competitors": [
                    {
                        "organization_id": c.organization_id,
                        "order": c.sort_order,
                    }
                    for c in competitors
                ],
            }
            f.write(sep)
            json.dump(line, f)
            sep = "\n"


def _delete_segments(cursor, industry_id: int, organization_id: str):
    num = cursor.execute(
        """delete from disruptor_categories
        where disruptor_categories.disruptor_id in (
        select d.id disruptor_id from disruptor d
        join crunchbase_initial_company ci
        on d.company_company_id = ci.company_company_id
        where d.report_id = %s and ci.organization_id = %s)
        """,
        (industry_id, organization_id),
    )
    logging.info(
        f"Deleted {num} segments of {organization_id} "
        f"in industry {industry_id}"
    )


def _add_segments(
    cursor,
    industry_id: int,
    organization_id: str,
    segment_entry_ids: list[str],
):
    if not segment_entry_ids:
        return

    num = cursor.execute(
        f"""insert into disruptor_categories
        select distinct d.id, cc.id
        from disruptor d
        join crunchbase_initial_company ci
        on d.company_company_id = ci.company_company_id and d.report_id=%s
        cross join company_category cc
        where
        ci.organization_id = %s and
        cc.external_entry_id in ({', '.join(['%s'] * len(segment_entry_ids))});
        """,
        [industry_id, organization_id] + segment_entry_ids,
    )
    assert num == len(segment_entry_ids)


def _delete_competitors(cursor, industry_id: int, organization_id: str):
    num = cursor.execute(
        """delete from disruptor_competitor_companies
    where disruptor_competitor_companies.disruptor_id in (
    select d.id
    from disruptor d
    join crunchbase_initial_company ci
    on d.company_company_id = ci.company_company_id
    and d.report_id = %s
    and ci.organization_id = %s
    );""",
        (industry_id, organization_id),
    )
    logging.info(
        f"Deleted {num} competitors of {organization_id} "
        f"in industry {industry_id}"
    )


def _resolve_company_id(cursor, organization_id):
    cursor.execute(
        """select company_company_id
        from crunchbase_initial_company
        where organization_id = %s
        """,
        (organization_id,),
    )
    return cursor.fetchone()["company_company_id"]


def _resolve_disruptor_id(cursor, industry_id, organization_id):
    cursor.execute(
        """select d.id from disruptor d
        join crunchbase_initial_company ci
        on d.company_company_id = ci.company_company_id
        and d.report_id = %s
        and ci.organization_id = %s""",
        (industry_id, organization_id),
    )
    return cursor.fetchone()["id"]


def _add_competitors(
    cursor, industry_id: int, organization_id: str, competitors
):
    if not competitors:
        return
    disruptor_id = _resolve_disruptor_id(cursor, industry_id, organization_id)
    new_competitors = [
        {
            "company_id": _resolve_company_id(cursor, c["organization_id"]),
            "order": c["order"],
        }
        for c in competitors
    ]
    num = cursor.execute(
        f"""insert into disruptor_competitor_companies(
        disruptor_id,
        competitor_companies_company_id,
        competitor_companies_order)
        values {', '.join([
           f'({disruptor_id}, {c["company_id"]}, {c["order"]})'
           for c in new_competitors
        ])}
        """
    )
    assert num == len(competitors)


def _update(connection, options):
    with open(options.update) as f, connection:
        for record in [json.loads(r) for r in f.readlines()]:
            with connection.cursor() as cursor:
                industry_id = record["industry_id"]
                organization_id = record["organization_id"]
                _delete_segments(cursor, industry_id, organization_id)
                _add_segments(
                    cursor,
                    industry_id,
                    organization_id,
                    record["segments"],
                )
                _delete_competitors(cursor, industry_id, organization_id)
                _add_competitors(
                    cursor, industry_id, organization_id, record["competitors"]
                )
        connection.commit()


def main():
    options = _parse_args(sys.argv[1:])
    logging.basicConfig(level=logging.INFO)
    connection = _connect(
        options.host, options.port, options.userpass, options.db
    )
    if options.command == "extract":
        _extract(connection, options)
    elif options.command == "update":
        _update(connection, options)
    else:
        logging.error(f"Unknown command: {options.command}")
        sys.exit(1)
