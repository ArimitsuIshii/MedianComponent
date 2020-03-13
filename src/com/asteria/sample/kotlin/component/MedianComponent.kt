package com.asteria.sample.kotlin.component

import com.infoteria.asteria.flowengine2.execute.ExecuteContext
import com.infoteria.asteria.flowlibrary2.FlowException
import com.infoteria.asteria.flowlibrary2.component.ComponentExceptionByMessageCode
import com.infoteria.asteria.flowlibrary2.component.SimpleComponent
import com.infoteria.asteria.flowlibrary2.property.IntegerProperty
import com.infoteria.asteria.flowlibrary2.property.StringProperty
import com.infoteria.asteria.flowlibrary2.stream.StreamType
import com.infoteria.asteria.util.StringUtil
import java.util.*

class MedianComponent : SimpleComponent {

    private val ERROR_FIELD_NAME_IS_EMPTY = "1"
    private val INPUT_STREAM_IS_EMPTY = "2"

    private val _propFieldName =
        StringProperty("FieldName", true, true)
    private val _propMedian =
        StringProperty("Median", false, true)
    private val _propRecordCount =
        IntegerProperty("RecordCount", false, true)

    constructor() : super() {
        inputConnector.acceptType = StreamType.RECORDS or StreamType.CSV
        inputConnector.acceptLinkCount = 1
        outputConnector.acceptType = StreamType.RECORDS or StreamType.CSV

        registerProperty(_propFieldName)
        registerProperty(_propMedian)
        registerProperty(_propRecordCount)
    }

    override fun getComponentName(): String {
        return "Median"
    }

    override fun execute(p0: ExecuteContext?): Boolean {
        //対象フィールドインデックスを取得
        val targetFieldIndex: Int = getTargetFieldIndex()

        //レコードのフィールド情報取得
        val record = inputConnector.stream.record
            ?: throw ComponentExceptionByMessageCode(this, INPUT_STREAM_IS_EMPTY)

        //レコード全部読み
        val valueList: MutableList<Double> = ArrayList()
        do {
            val fieldValue = record.getValue(targetFieldIndex)
            if (fieldValue.isNumberType && !fieldValue.isNull) {
                val doubleValue = fieldValue.doubleValue()
                valueList.add(doubleValue)
            }
        } while (record.next())
        valueList.sort()
        val size = valueList.size
        //レコード数をプロパティへ設定
        _propRecordCount.setValue(size.toLong())
        var value: Double = 0.0
        val one = size / 2
        if (one > 0) {
            value = if (size % 2 == 1) {
                valueList[one]
            } else {
                (valueList[one - 1] + valueList[one]) / 2
            }
        } else {
            value = valueList[0]
        }
        //中央値をプロパティへ設定
        _propMedian.setValue(value.toString())

        passStream();
        return true
    }

    @Throws(FlowException::class)
    private fun getTargetFieldIndex(): Int {
        val targetFieldName = _propFieldName.strValue()
        if (StringUtil.isEmpty(targetFieldName)) {
            throw ComponentExceptionByMessageCode(this, ERROR_FIELD_NAME_IS_EMPTY)
        }
        return inputConnector.stream.fieldDefinition.indexOfName(targetFieldName)
    }

}