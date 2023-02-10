package com.dtstack.chunjun.connector.filejson.source;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.filejson.conf.FileJsonConf;
import com.dtstack.chunjun.connector.filejson.converter.FileJsonColumnConverter;
import com.dtstack.chunjun.connector.filejson.converter.FileJsonRawTypeConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

public class FilejsonSourceFactory extends SourceFactory {
    private final FileJsonConf fileJsonConf;

    public FilejsonSourceFactory(SyncConf config, StreamExecutionEnvironment env) {
        super(config, env);
        fileJsonConf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(config.getReader().getParameter()),
                        FileJsonConf.class);
        fileJsonConf.setColumn(config.getReader().getFieldList());
        super.initCommonConf(fileJsonConf);
    }

    @Override
    public DataStream<RowData> createSource() {
        FileJsonInputFormatBuilder builder = new FileJsonInputFormatBuilder();
        builder.setFileJsonConf(fileJsonConf);
        //        final RowType rowType =
        //                TableUtil.createRowType(fileJsonConf.getColumn(), getRawTypeConverter());
        builder.setRowConverter(
                new FileJsonColumnConverter(fileJsonConf.getColumn(), fileJsonConf),
                useAbstractBaseColumn);

        return createInput(builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return FileJsonRawTypeConverter::apply;
    }
}
