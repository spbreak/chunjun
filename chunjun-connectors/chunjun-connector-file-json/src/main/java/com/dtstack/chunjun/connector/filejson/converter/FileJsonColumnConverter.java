package com.dtstack.chunjun.connector.filejson.converter;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.filejson.conf.FileJsonConf;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.decoder.IDecode;
import com.dtstack.chunjun.decoder.TextDecoder;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.MapColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;
import com.dtstack.chunjun.util.DateUtil;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class FileJsonColumnConverter
        extends AbstractRowConverter<String, RowData, String[], String> {

    private DeserializationSchema<RowData> valueDeserialization;

    public FileJsonColumnConverter(List<FieldConf> fieldConfList, FileJsonConf fileJsonConf) {
        super(fieldConfList.size(), fileJsonConf);
        for (int i = 0; i < fieldConfList.size(); i++) {
            String type = fieldConfList.get(i).getType();
            int left = type.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL);
            int right = type.indexOf(ConstantValue.RIGHT_PARENTHESIS_SYMBOL);
            if (left > 0 && right > 0) {
                type = type.substring(0, left);
            }
            IDeserializationConverter iDeserializationConverter =  createInternalConverter(type);
            IDeserializationConverter iDeserializationConverterWrap = wrapIntoNullableInternalConverter(iDeserializationConverter);
            toInternalConverters.add(iDeserializationConverterWrap);

            ISerializationConverter iSerializationConverter = createExternalConverter(type);
            ISerializationConverter<String[]> iSerializationConverterWrap = wrapIntoNullableExternalConverter(iSerializationConverter, type);
            toExternalConverters.add(iSerializationConverterWrap);
        }
    }

    @Override
    public RowData toInternal(String input) throws Exception {
        ColumnRowData row = new ColumnRowData(commonConf.getColumn().size()); 
        //CSV文件用逗号分割, 定长文件按元数据位置
        String[] strings = input.split(",");
        row.addField(new BigDecimalColumn(strings[0]));
        row.addField(new StringColumn(strings[1]));
        return row;
    }

    @Override
    public String[] toExternal(RowData rowData, String[] data) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters.get(index).serialize(rowData, index, data);
        }
        return data;
    }

    @Override
    public RowData toInternalLookup(RowData input) {
        throw new ChunJunRuntimeException(
                "FileJson Connector doesn't support Lookup Table Function.");
    }

    @Override
    public IDeserializationConverter wrapIntoNullableInternalConverter(
            IDeserializationConverter IDeserializationConverter) {
        return val -> {
            if (val == null || "".equals(val)) {
                return null;
            } else {
                try {
                    return IDeserializationConverter.deserialize(val);
                } catch (Exception e) {
                    LOG.error("value [{}] convent failed ", val);
                    throw e;
                }
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public ISerializationConverter<String[]> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, String type) {
        return (rowData, index, data) -> {
            if (rowData == null || rowData.isNullAt(index)) {
                data[index] = "\\N";
            } else {
                serializationConverter.serialize(rowData, index, data);
            }
        };
    }

    @Override
    public IDeserializationConverter createInternalConverter(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new BooleanColumn(Boolean.parseBoolean(val));
            case "TINYINT":
                return (IDeserializationConverter<Byte, AbstractBaseColumn>) ByteColumn::new;
            case "SMALLINT":
            case "INT":
            case "BIGINT":
            case "FLOAT":
            case "DOUBLE":
            case "DECIMAL":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        BigDecimalColumn::new;
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                return (IDeserializationConverter<String, AbstractBaseColumn>) StringColumn::new;
            case "TIMESTAMP":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> {
                            try {
                                return new TimestampColumn(
                                        Timestamp.valueOf(val),
                                        DateUtil.getPrecisionFromTimestampStr(val));
                            } catch (Exception e) {
                                return new TimestampColumn(DateUtil.getTimestampFromStr(val), 0);
                            }
                        };
            case "DATE":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> {
                            Timestamp timestamp = DateUtil.getTimestampFromStr(val);
                            if (timestamp == null) {
                                return new SqlDateColumn(null);
                            } else {
                                return new SqlDateColumn(
                                        Date.valueOf(timestamp.toLocalDateTime().toLocalDate()));
                            }
                        };
            case "BINARY":
            case "ARRAY":
            case "MAP":
            case "STRUCT":
            case "UNION":
            default:
                throw new UnsupportedTypeException(type);
        }
    }

    @Override
    public ISerializationConverter<String[]> createExternalConverter(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
                return (rowData, index, data) ->
                        data[index] = String.valueOf(rowData.getBoolean(index));
            case "TINYINT":
                return (rowData, index, data) ->
                        data[index] = String.valueOf(rowData.getByte(index));
            case "SMALLINT":
                return (rowData, index, data) ->
                        data[index] = String.valueOf(rowData.getShort(index));
            case "INT":
                return (rowData, index, data) ->
                        data[index] = String.valueOf(rowData.getInt(index));
            case "BIGINT":
                return (rowData, index, data) ->
                        data[index] = String.valueOf(rowData.getLong(index));
            case "FLOAT":
                return (rowData, index, data) ->
                        data[index] = String.valueOf(rowData.getFloat(index));
            case "DOUBLE":
                return (rowData, index, data) ->
                        data[index] = String.valueOf(rowData.getDouble(index));
            case "DECIMAL":
                return (rowData, index, data) ->
                        data[index] = String.valueOf(rowData.getDecimal(index, 38, 18));
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                return (rowData, index, data) ->
                        data[index] = String.valueOf(rowData.getString(index));
            case "TIMESTAMP":
                return (rowData, index, data) -> {
                    AbstractBaseColumn field = ((ColumnRowData) rowData).getField(index);
                    data[index] = field.asTimestampStr();
                };
            case "DATE":
                return (rowData, index, data) ->
                        data[index] =
                                String.valueOf(
                                        new Date(rowData.getTimestamp(index, 6).getMillisecond()));
            case "BINARY":
                return (rowData, index, data) ->
                        data[index] = Arrays.toString(rowData.getBinary(index));
            case "ARRAY":
            case "MAP":
            case "STRUCT":
            case "UNION":
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
