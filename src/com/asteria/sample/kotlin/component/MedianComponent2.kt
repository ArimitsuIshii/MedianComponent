package com.asteria.sample.kotlin.component

import com.infoteria.asteria.flowengine2.execute.ExecuteContext
import com.infoteria.asteria.flowlibrary2.FlowException
import com.infoteria.asteria.flowlibrary2.component.ComponentExceptionByMessageCode
import com.infoteria.asteria.flowlibrary2.component.SimpleComponent
import com.infoteria.asteria.flowlibrary2.property.StringProperty
import com.infoteria.asteria.flowlibrary2.stream.StreamType
import com.infoteria.asteria.util.StringUtil

class MedianComponent2 : SimpleComponent {

    private val ERROR_FIELD_NAME_IS_EMPTY = "1"
    private val INPUT_STREAM_IS_EMPTY = "2"

    private val _propNameField =
        StringProperty("NameField", true, true)
    private val _propValueField =
        StringProperty("ValueField", true, true)
    private val _propMedianName =
        StringProperty("MedianName", false, true)
    private val _propMedian =
        StringProperty("Median", false, true)

    constructor() : super() {
        inputConnector.acceptType = StreamType.RECORDS or StreamType.CSV
        inputConnector.acceptLinkCount = 1
        outputConnector.acceptType = StreamType.RECORDS or StreamType.CSV

        registerProperty(_propNameField)
        registerProperty(_propValueField)
        registerProperty(_propMedianName)
        registerProperty(_propMedian)
    }

    override fun getComponentName(): String {
        return "Median2"
    }

    override fun execute(p0: ExecuteContext?): Boolean {
        //対象の値フィールドインデックスを取得
        val targetValueFieldIndex: Int = getFieldIndex(_propValueField)

        //レコードのフィールド情報取得
        val record = inputConnector.stream.record
            ?: throw ComponentExceptionByMessageCode(this, INPUT_STREAM_IS_EMPTY)

        //レコード全部読み
        var allValue: Double = 0.0
        do {
            val fieldValue = record.getValue(targetValueFieldIndex)
            if (fieldValue.isNumberType && !fieldValue.isNull) {
                allValue = allValue + fieldValue.doubleValue()
            }
        } while (record.next())

        //中央値をプロパティへ設定
        val center: Double = allValue / 2
        _propMedian.setValue(center.toString())

        //はじめに戻る
        record.first()
        allValue = 0.0

        do {
            val fieldValue = record.getValue(targetValueFieldIndex)
            if (fieldValue.isNumberType && !fieldValue.isNull) {
                allValue = allValue + fieldValue.doubleValue()
                if (allValue > center) {
                    //中央値名をプロパティへ設定
                    _propMedianName.setValue(record.getValue(getFieldIndex(_propNameField)).strValue())
                    break;
                }
            }
        } while (record.next())

        passStream();
        return true
    }

    @Throws(FlowException::class)
    private fun getFieldIndex(property : StringProperty): Int {
        val targetValueField = property.strValue()
        if (StringUtil.isEmpty(targetValueField)) {
            throw ComponentExceptionByMessageCode(this, ERROR_FIELD_NAME_IS_EMPTY)
        }
        return inputConnector.stream.fieldDefinition.indexOfName(targetValueField)
    }
}