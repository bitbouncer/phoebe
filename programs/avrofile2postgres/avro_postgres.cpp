#include <algorithm>
#include <string>
#include <boost/make_shared.hpp>
#include <avro/Specific.hh>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include <avro/Compiler.hh>
#include <avro/Schema.hh>
#include <avro/AvroSerialize.hh>
#include "avro_postgres.h"

#include <iostream>

//inpiration
//http://upp-mirror.googlecode.com/svn/trunk/uppsrc/PostgreSQL/PostgreSQL.cpp

/*
avro::NodePtr schema_for_date()
{
return boost::make_shared<avro::Node>(avro::AVRO_STRING);
}

avro::NodePtr schema_for_date()
{
return boost::make_shared<avro::Node>(avro::AVRO_STRING);
}

avro::NodePtr schema_for_record()
{
return boost::make_shared<avro::Node>(avro::AVRO_RECORD);
}
*/

/* Generates an Avro schema that can be used to encode a Postgres type
* with the given OID. */
/*
avro::Schema schema_for_oid2(Oid typid)
{
avro::Schema  value_schema = boost::make_shared<avro::Node>(avro::AVRO_BOOL);
}
*/

//avro::NodePtr schema_for_oid(Oid typid)
//{
//    avro::NodePtr  value_schema;
//    switch ((PG_OIDS) typid) {
//        /* Numeric-like types */
//    case BOOLOID:    /* boolean: 'true'/'false' */
//        value_schema = boost::make_shared<avro::Node>(avro::AVRO_BOOL);
//        break;
//    case FLOAT4OID:  /* real, float4: 32-bit floating point number */
//        value_schema = boost::make_shared<avro::Node>(avro::AVRO_FLOAT);
//        break;
//    case FLOAT8OID:  /* double precision, float8: 64-bit floating point number */
//        value_schema = boost::make_shared<avro::Node>(avro::AVRO_DOUBLE);
//        break;
//    case INT2OID:    /* smallint, int2: 16-bit signed integer */
//    case INT4OID:    /* integer, int, int4: 32-bit signed integer */
//        value_schema = boost::make_shared<avro::Node>(avro::AVRO_INT);
//        break;
//    case INT8OID:    /* bigint, int8: 64-bit signed integer */
//    //case CASHOID:    /* money: monetary amounts, $d,ddd.cc, stored as 64-bit signed integer */
//    case OIDOID:     /* oid: Oid is unsigned int */
//    //case REGPROCOID: /* regproc: RegProcedure is Oid */
//    case XIDOID:     /* xid: TransactionId is uint32 */
//    case CIDOID:     /* cid: CommandId is uint32 */
//        value_schema = boost::make_shared<avro::Node>(avro::AVRO_LONG);
//        break;
//    case NUMERICOID: /* numeric(p, s), decimal(p, s): arbitrary precision number */
//        value_schema = boost::make_shared<avro::Node>(avro::AVRO_DOUBLE);
//        break;
//
//        /* Date/time types. We don't bother with abstime, reltime and tinterval (which are based
//        * on Unix timestamps with 1-second resolution), as they are deprecated. */
//
//    //case DATEOID:        /* date: 32-bit signed integer, resolution of 1 day */
//    //    //return schema_for_date(); not implemented YET
//    //    value_schema = boost::make_shared<avro::Node>(avro::AVRO_STRING);
//    //    break;
//    //    // this is wrong...
//
//    case TIMEOID:        /* time without time zone: microseconds since start of day */
//        value_schema = boost::make_shared<avro::Node>(avro::AVRO_LONG);
//        break;
//    //case TIMETZOID:      /* time with time zone, timetz: time of day with time zone */
//    //    //value_schema = schema_for_time_tz(); NOT IMPEMENTED YET
//    //    value_schema = boost::make_shared<avro::Node>(avro::AVRO_STRING);
//    //    break;
//    //case TIMESTAMPOID:   /* timestamp without time zone: datetime, microseconds since epoch */
//    //    // return schema_for_timestamp(false);NOT IMPEMENTED YET
//    //    value_schema = boost::make_shared<avro::Node>(avro::AVRO_STRING);
//    //    break;
//    //case TIMESTAMPTZOID: /* timestamp with time zone, timestamptz: datetime with time zone */
//    //    //return schema_for_timestamp(true); NOT IMPEMENTED YET
//    //    value_schema = boost::make_shared<avro::Node>(avro::AVRO_STRING);
//    //    break;
//
//    //case INTERVALOID:    /* @ <number> <units>, time interval */
//    //    //value_schema = schema_for_interval(); NOT IMPEMENTED YET
//    //    value_schema = boost::make_shared<avro::Node>(avro::AVRO_STRING);
//    //    break;
//
//        /* Binary string types */
//    case BYTEAOID:   /* bytea: variable-length byte array */
//        value_schema = boost::make_shared<avro::Node>(avro::AVRO_BYTES);
//        break;
//    //case BITOID:     /* fixed-length bit string */
//    //case VARBITOID:  /* variable-length bit string */
//    //case UUIDOID:    /* UUID datatype */
//
//        //case LSNOID:     /* PostgreSQL LSN datatype */
//    //case MACADDROID: /* XX:XX:XX:XX:XX:XX, MAC address */
//    //case INETOID:    /* IP address/netmask, host address, netmask optional */
//    //case CIDROID:    /* network IP address/netmask, network address */
//
//        /* Geometric types */
//    //case POINTOID:   /* geometric point '(x, y)' */
//    //case LSEGOID:    /* geometric line segment '(pt1,pt2)' */
//    //case PATHOID:    /* geometric path '(pt1,...)' */
//    //case BOXOID:     /* geometric box '(lower left,upper right)' */
//    //case POLYGONOID: /* geometric polygon '(pt1,...)' */
//    //case LINEOID:    /* geometric line */
//    //case CIRCLEOID:  /* geometric circle '(center,radius)' */
//
//    /* range types... decompose like array types? */
//
//    /* JSON types */
//    //case JSONOID:    /* json: Text-based JSON */
//    //case JSONBOID:   /* jsonb: Binary JSON */
//
//        /* String-like types: fall through to the default, which is to create a string representation */
//    case CHAROID:    /* "char": single character */
//    case NAMEOID:    /* name: 63-byte type for storing system identifiers */
//    case TEXTOID:    /* text: variable-length string, no limit specified */
//    //case BPCHAROID:  /* character(n), char(length): blank-padded string, fixed storage length */
//    //case VARCHAROID: /* varchar(length): non-blank-padded string, variable storage length */
//    default:
//        value_schema = boost::make_shared<avro::Node>(avro::AVRO_STRING);
//        break;
//    }
//
//    /* Make a union of value_schema with null. Some types are already a union,
//    * in which case they must include null as the first branch of the union,
//    * and return directly from the function without getting here (otherwise
//    * we'd get a union inside a union, which is not valid Avro). */
//    avro::NodePtr null_schema = boost::make_shared<avro::Node>(avro::AVRO_NULL);
//    avro::NodePtr union_schema = boost::make_shared<avro::Node>(avro::AVRO_UNION);
//    union_schema->addLeaf(null_schema);
//    union_schema->addLeaf(value_schema);
//    //avro_schema_decref(null_schema);
//    //avro_schema_decref(value_schema);
//    return union_schema;
//}

boost::shared_ptr<avro::Schema>  schema_for_oid(Oid typid)
{
    boost::shared_ptr<avro::Schema>  value_schema;
    switch ((PG_OIDS)typid) {
        /* Numeric-like types */
    case BOOLOID:    /* boolean: 'true'/'false' */
        value_schema = boost::make_shared<avro::BoolSchema>();
        break;
    case FLOAT4OID:  /* real, float4: 32-bit floating point number */
        value_schema = boost::make_shared<avro::FloatSchema>();
        break;
    case FLOAT8OID:  /* double precision, float8: 64-bit floating point number */
        value_schema = boost::make_shared<avro::DoubleSchema>();
        break;
    case INT2OID:    /* smallint, int2: 16-bit signed integer */
    case INT4OID:    /* integer, int, int4: 32-bit signed integer */
        value_schema = boost::make_shared<avro::IntSchema>();
        break;
    case INT8OID:    /* bigint, int8: 64-bit signed integer */
        //case CASHOID:    /* money: monetary amounts, $d,ddd.cc, stored as 64-bit signed integer */
    case OIDOID:     /* oid: Oid is unsigned int */
        //case REGPROCOID: /* regproc: RegProcedure is Oid */
    case XIDOID:     /* xid: TransactionId is uint32 */
    case CIDOID:     /* cid: CommandId is uint32 */
        value_schema = boost::make_shared<avro::LongSchema>();
        break;
    case NUMERICOID: /* numeric(p, s), decimal(p, s): arbitrary precision number */
        value_schema = boost::make_shared<avro::DoubleSchema>();
        break;

        /* Date/time types. We don't bother with abstime, reltime and tinterval (which are based
        * on Unix timestamps with 1-second resolution), as they are deprecated. */

        //case DATEOID:        /* date: 32-bit signed integer, resolution of 1 day */
        //    //return schema_for_date(); not implemented YET
        //    value_schema = boost::make_shared<avro::Node>(avro::AVRO_STRING);
        //    break;
        //    // this is wrong...

    case TIMEOID:        /* time without time zone: microseconds since start of day */
        value_schema = boost::make_shared<avro::LongSchema>();
        break;
        //case TIMETZOID:      /* time with time zone, timetz: time of day with time zone */
        //    //value_schema = schema_for_time_tz(); NOT IMPEMENTED YET
        //    value_schema = boost::make_shared<avro::Node>(avro::AVRO_STRING);
        //    break;
        //case TIMESTAMPOID:   /* timestamp without time zone: datetime, microseconds since epoch */
        //    // return schema_for_timestamp(false);NOT IMPEMENTED YET
        //    value_schema = boost::make_shared<avro::Node>(avro::AVRO_STRING);
        //    break;
        //case TIMESTAMPTZOID: /* timestamp with time zone, timestamptz: datetime with time zone */
        //    //return schema_for_timestamp(true); NOT IMPEMENTED YET
        //    value_schema = boost::make_shared<avro::Node>(avro::AVRO_STRING);
        //    break;

        //case INTERVALOID:    /* @ <number> <units>, time interval */
        //    //value_schema = schema_for_interval(); NOT IMPEMENTED YET
        //    value_schema = boost::make_shared<avro::Node>(avro::AVRO_STRING);
        //    break;

        /* Binary string types */
    case BYTEAOID:   /* bytea: variable-length byte array */
        value_schema = boost::make_shared<avro::BytesSchema>();
        break;
        //case BITOID:     /* fixed-length bit string */
        //case VARBITOID:  /* variable-length bit string */
        //case UUIDOID:    /* UUID datatype */

        //case LSNOID:     /* PostgreSQL LSN datatype */
        //case MACADDROID: /* XX:XX:XX:XX:XX:XX, MAC address */
        //case INETOID:    /* IP address/netmask, host address, netmask optional */
        //case CIDROID:    /* network IP address/netmask, network address */

        /* Geometric types */
        //case POINTOID:   /* geometric point '(x, y)' */
        //case LSEGOID:    /* geometric line segment '(pt1,pt2)' */
        //case PATHOID:    /* geometric path '(pt1,...)' */
        //case BOXOID:     /* geometric box '(lower left,upper right)' */
        //case POLYGONOID: /* geometric polygon '(pt1,...)' */
        //case LINEOID:    /* geometric line */
        //case CIRCLEOID:  /* geometric circle '(center,radius)' */

        /* range types... decompose like array types? */

        /* JSON types */
        //case JSONOID:    /* json: Text-based JSON */
        //case JSONBOID:   /* jsonb: Binary JSON */

        /* String-like types: fall through to the default, which is to create a string representation */
    case CHAROID:    /* "char": single character */
    case NAMEOID:    /* name: 63-byte type for storing system identifiers */
    case TEXTOID:    /* text: variable-length string, no limit specified */
        //case BPCHAROID:  /* character(n), char(length): blank-padded string, fixed storage length */
        //case VARCHAROID: /* varchar(length): non-blank-padded string, variable storage length */
    default:
        value_schema = boost::make_shared<avro::StringSchema>();
        break;
    }

    /* Make a union of value_schema with null. Some types are already a union,
    * in which case they must include null as the first branch of the union,
    * and return directly from the function without getting here (otherwise
    * we'd get a union inside a union, which is not valid Avro). */
    boost::shared_ptr<avro::Schema> null_schema = boost::make_shared<avro::NullSchema>();
    boost::shared_ptr<avro::UnionSchema> union_schema = boost::make_shared<avro::UnionSchema>();
    union_schema->addType(*null_schema);
    union_schema->addType(*value_schema);
    //avro_schema_decref(null_schema);
    //avro_schema_decref(value_schema);
    return union_schema;
}


boost::shared_ptr<avro::RecordSchema> schema_for_table_row(std::string schema_name, boost::shared_ptr<PGresult> res)
{
    boost::shared_ptr<avro::RecordSchema> record_schema = boost::make_shared<avro::RecordSchema>(schema_name);

    int nFields = PQnfields(res.get());
    for (int i = 0; i < nFields; i++)
    {
        Oid col_type = PQftype(res.get(), i);
        std::string col_name = PQfname(res.get(), i);
        boost::shared_ptr<avro::Schema> col_schema = schema_for_oid(col_type);
        /* TODO ensure that names abide by Avro's requirements */
        record_schema->addField(col_name, *col_schema);
    }
    return record_schema;
}

boost::shared_ptr<avro::RecordSchema> schema_for_table_key(std::string schema_name, const std::vector<std::string>& keys, boost::shared_ptr<PGresult> res)
{
    boost::shared_ptr<avro::RecordSchema> record_schema = boost::make_shared<avro::RecordSchema>(schema_name);
    for (std::vector<std::string>::const_iterator i = keys.begin(); i != keys.end(); i++)
    {
        int column_index = PQfnumber(res.get(), i->c_str());
        assert(column_index >= 0);
        if (column_index >= 0)
        {
            Oid col_type = PQftype(res.get(), column_index);
            boost::shared_ptr<avro::Schema> col_schema = schema_for_oid(col_type);
            /* TODO ensure that names abide by Avro's requirements */
            record_schema->addField(*i, *col_schema);
        }
    }
    return record_schema;
}

boost::shared_ptr<avro::ValidSchema>  valid_schema_for_table_row(std::string schema_name, boost::shared_ptr<PGresult> res)
{
    return boost::make_shared<avro::ValidSchema>(*schema_for_table_row(schema_name, res));
}

boost::shared_ptr<avro::ValidSchema>  valid_schema_for_table_key(std::string schema_name, const std::vector<std::string>& keys, boost::shared_ptr<PGresult> res)
{
    return boost::make_shared<avro::ValidSchema>(*schema_for_table_key(schema_name, keys, res));
}

// this is done by assuming all fields are in the same order... ???
std::vector<boost::shared_ptr<avro::GenericDatum>> to_avro(boost::shared_ptr<avro::ValidSchema> schema, boost::shared_ptr<PGresult> res)
{
    size_t nFields = PQnfields(res.get());
    int nRows = PQntuples(res.get());
    std::vector<boost::shared_ptr<avro::GenericDatum>> result;
    result.reserve(nRows);
    for (int i = 0; i < nRows; i++)
    {
        boost::shared_ptr<avro::GenericDatum> gd = boost::make_shared<avro::GenericDatum>(*schema);
        assert(gd->type() == avro::AVRO_RECORD); 
        avro::GenericRecord& record(gd->value<avro::GenericRecord>());
        for (int j = 0; j < nFields; j++)
        {
            if (record.fieldAt(j).type() != avro::AVRO_UNION)
            {
                std::cerr << "unexpected schema - bailing out" << std::endl;
                break;
            }

            avro::GenericUnion& au(record.fieldAt(j).value<avro::GenericUnion>());

            if (PQgetisnull(res.get(), i, j) == 1)
            {
                au.selectBranch(0); // NULL branch - we hope..
                assert(au.datum().type() == avro::AVRO_NULL);
            }
            else
            {
                au.selectBranch(1);
                avro::GenericDatum& avro_item(au.datum());
                const char* val = PQgetvalue(res.get(), i, j);

                switch (avro_item.type())
                {
                case avro::AVRO_STRING:
                    avro_item.value<std::string>() = val;
                    break;
                case avro::AVRO_BYTES:
                    avro_item.value<std::string>() = val;
                    break;
                case avro::AVRO_INT:
                    avro_item.value<int32_t>() = atoi(val);
                    break;
                case avro::AVRO_LONG:
                    avro_item.value<int64_t>() = std::stoull(val);
                    break;
                case avro::AVRO_FLOAT:
                    avro_item.value<float>() = (float)atof(val);
                    break;
                case avro::AVRO_DOUBLE:
                    avro_item.value<double>() = atof(val);
                    break;
                case avro::AVRO_BOOL:
                    avro_item.value<bool>() = (strcmp(val, "True") == 0);
                    break;
                case avro::AVRO_RECORD:
                case avro::AVRO_ENUM:
                case avro::AVRO_ARRAY:
                case avro::AVRO_MAP:
                case avro::AVRO_UNION:
                case avro::AVRO_FIXED:
                case avro::AVRO_NULL:
                default:
                    std::cerr << "unexpectd / non supported type e:" << avro_item.type() << std::endl;;
                    assert(false);
                }
            }
        }
        result.push_back(gd);

        //std::cerr << std::endl;

         //avro::encode(*encoder, *gd);
        // I create a DataFileWriter and i write my pair of ValidSchema and GenericValue
        //avro::DataFileWriter<Pair> dataFileWriter("test.bin", schema);
        //dataFileWriter.write(p);
        //dataFileWriter.close();
    }
    return result;
}

// this is done by mapping schema field names to result columns...
std::vector<boost::shared_ptr<avro::GenericDatum>> to_avro2(boost::shared_ptr<avro::ValidSchema> schema, boost::shared_ptr<PGresult> res)
{
    int nRows = PQntuples(res.get());
    std::vector<boost::shared_ptr<avro::GenericDatum>> result;
    result.reserve(nRows);
    for (int i = 0; i < nRows; i++)
    {
        boost::shared_ptr<avro::GenericDatum> gd = boost::make_shared<avro::GenericDatum>(*schema);
        assert(gd->type() == avro::AVRO_RECORD);
        avro::GenericRecord& record(gd->value<avro::GenericRecord>());
        size_t nFields = record.fieldCount();
        for (int j = 0; j < nFields; j++)
        {
            if (record.fieldAt(j).type() != avro::AVRO_UNION)
            {
                std::cerr << "unexpected schema - bailing out, type:" << record.fieldAt(j).type() << std::endl;
                assert(false);
                break;
            }
            avro::GenericUnion& au(record.fieldAt(j).value<avro::GenericUnion>());

            const std::string& column_name = record.schema()->nameAt(j);

            //which pg column has this value?
            int column_index = PQfnumber(res.get(), column_name.c_str());
            if (column_index < 0)
            {
                std::cerr << "unknown column - bailing out: " << column_name << std::endl;
                assert(false);
                break;
            }

            if (PQgetisnull(res.get(), i, column_index) == 1)
            {
                au.selectBranch(0); // NULL branch - we hope..
                assert(au.datum().type() == avro::AVRO_NULL);
            }
            else
            {
                au.selectBranch(1);
                avro::GenericDatum& avro_item(au.datum());
                const char* val = PQgetvalue(res.get(), i, j);

                switch (avro_item.type())
                {
                case avro::AVRO_STRING:
                    avro_item.value<std::string>() = val;
                    break;
                case avro::AVRO_BYTES:
                    avro_item.value<std::string>() = val;
                    break;
                case avro::AVRO_INT:
                    avro_item.value<int32_t>() = atoi(val);
                    break;
                case avro::AVRO_LONG:
                    avro_item.value<int64_t>() = std::stoull(val);
                    break;
                case avro::AVRO_FLOAT:
                    avro_item.value<float>() = (float)atof(val);
                    break;
                case avro::AVRO_DOUBLE:
                    avro_item.value<double>() = atof(val);
                    break;
                case avro::AVRO_BOOL:
                    avro_item.value<bool>() = (strcmp(val, "True") == 0);
                    break;
                case avro::AVRO_RECORD:
                case avro::AVRO_ENUM:
                case avro::AVRO_ARRAY:
                case avro::AVRO_MAP:
                case avro::AVRO_UNION:
                case avro::AVRO_FIXED:
                case avro::AVRO_NULL:
                default:
                    std::cerr << "unexpectd / non supported type e:" << avro_item.type() << std::endl;;
                    assert(false);
                }
            }
        }
        result.push_back(gd);

        //std::cerr << std::endl;

        //avro::encode(*encoder, *gd);
        // I create a DataFileWriter and i write my pair of ValidSchema and GenericValue
        //avro::DataFileWriter<Pair> dataFileWriter("test.bin", schema);
        //dataFileWriter.write(p);
        //dataFileWriter.close();
    }
    return result;
}


/*
std::string postgres_type(avro::Type t)
{

}
*/


//std::string build_fields()
//{
//    auto schema = confound_orders_t::valid_schema();
//    avro::GenericDatum gd(schema);
//    auto t = gd.type();
//    assert(t == avro::AVRO_RECORD);
//    avro::GenericRecord gr = gd.value<avro::GenericRecord>();
//    for (int i = 0; i != gr.fieldCount(); i++)
//    {
//        std::cerr << i << " -> ";
//        std::cerr << gr.schema()->nameAt(i);
//        auto leaf = gr.schema()->leafAt(i);
//        switch (leaf->type())
//        {
//        case avro::AVRO_ARRAY:
//        case avro::AVRO_BOOL:
//        case avro::AVRO_BYTES:
//            std::cerr << ", type:" << leaf->type() << std::endl;
//
//        case avro::AVRO_UNION:
//
//            if (leaf->type() == avro::AVRO_UNION)
//            {
//                //this should be a NULLable field and the real type.
//                for (int j = 0; j != leaf->)
//
//            }
//        };
//        /*
//        auto f = gr.fieldAt(i);
//        if (f.isUnion())
//        {
//        std::cerr << f.isUnion() ? "union";
//        f.
//        }
//        */
//    }
//    return "*";
//}

/*
std::string escapeSQL(SQLConnection & connection, const std::string & dataIn)
{
    // This might be better as an assertion or exception, if an empty string
    // is considered an error. Depends on your requirements for this function.
    if (dataIn.empty())
    {
        return "";
    }

    const std::size_t dataInLen = dataIn.length();
    std::vector<char> temp((dataInLen * 2) + 1, '\0');
    mysql_real_escape_string(&connection, temp.data(), dataIn.c_str(), dataInLen);

    return temp.data();
    // Will create a new string but the compiler is likely 
    // to optimize this to a cheap move operation (C++11).
}
*/

class IsChars
{
public:
    IsChars(const char* charsToRemove) : chars(charsToRemove) {};

    bool operator()(char c)
    {
        for (const char* testChar = chars; *testChar != 0; ++testChar)
        {
            if (*testChar == c) { return true; }
        }
        return false;
    }

private:
    const char* chars;
};

std::string escapeSQLstring(std::string src)
{
    //we should escape the sql string instead of doing this... - for now this removes ' characters in string
    src.erase(std::remove_if(src.begin(), src.end(), IsChars("'")), src.end());
    return std::string("'" + src + "'"); // we have to do real escaping here to prevent injection attacks  TBD
}

std::string avro2sql_values(const avro::ValidSchema& schema, avro::GenericDatum& datum)
{
    std::string result = "(";
    assert(datum.type() == avro::AVRO_RECORD);
    avro::GenericRecord& record(datum.value<avro::GenericRecord>());
    size_t nFields = record.fieldCount();
    for (int i= 0; i < nFields; i++)
    {
        std::string val;

        if (record.fieldAt(i).type() != avro::AVRO_UNION)
        {
            std::cerr << "unexpected schema - bailing out" << std::endl;
            break;
        }

        avro::GenericUnion& au(record.fieldAt(i).value<avro::GenericUnion>());
        avro::GenericDatum& column = au.datum();
        auto t = column.type();
        switch (t)
        {
        case avro::AVRO_NULL:
            val = "NULL";
            break;
        case avro::AVRO_STRING:
            val = escapeSQLstring(column.value<std::string>());
            break;
        case avro::AVRO_BYTES:
            val = column.value<std::string>();
            break;
        case avro::AVRO_INT:
            val = std::to_string(column.value<int32_t>());
            break;
        case avro::AVRO_LONG:
            val = std::to_string(column.value<int64_t>());
            break;
        case avro::AVRO_FLOAT:
            val = std::to_string(column.value<float>());
            break;
        case avro::AVRO_DOUBLE:
            val = std::to_string(column.value<double>());
            break;
        case avro::AVRO_BOOL:
            val = column.value<bool>() ? "True" : "False";
            break;
        case avro::AVRO_RECORD:
        case avro::AVRO_ENUM:
        case avro::AVRO_ARRAY:
        case avro::AVRO_MAP:
        case avro::AVRO_UNION:
        case avro::AVRO_FIXED:
        default:
            std::cerr << "unexpected / non supported type e:" << column.type() << std::endl;;
            assert(false);
        }
        if (i < (nFields - 1))
            result += val + ", ";
        else
            result += val + ")";
    }
    return result;
}

std::string avro2sql_table_name(boost::shared_ptr<avro::ValidSchema> schema, avro::GenericDatum& datum)
{
    auto r = schema->root();
    assert(r->type() == avro::AVRO_RECORD);
    if (r->hasName())
    {
        std::string name = r->name();

        //since we use convention tablename.key / table_name.value (until we know what bottledwater does...)
        // return namesapace as table name
        std::size_t found = name.find_first_of(".");
        if (found != std::string::npos)
        {
            std::string ns = name.substr(0, found);
            return ns;
        }
        return r->name();
    }
    assert(false);
    return "unknown_table_name";
}

std::string avro2sql_column_names(const avro::ValidSchema& schema, avro::GenericDatum& datum)
{
    auto r = schema.root();
    assert(r->type() == avro::AVRO_RECORD);
    std::string s = "(";
    size_t sz = r->names();
    for (int i = 0; i != sz; ++i)
    {
        s += r->nameAt(i);
        if (i != sz - 1)
            s += ",";
    }
    s += ")";
    /*
    schema->toJson(std::cerr);
    std::cerr << std::endl;
    return "unknown";
    */
    return s;
}
