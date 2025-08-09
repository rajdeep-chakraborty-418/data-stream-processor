"""
Define Error Codes for Logging Integrations
"""
ALARM_STREAM_PROCESSOR_UNHANDLED_EXCEPTION: str = """
001_Stream_Processor_Unhandled_Exception_Alert: Error encountered during stream processing: {error}
"""
ALARM_STREAM_PROCESSOR_STREAM_DATAFRAME_EXCEPTION: str = """
002_Stream_Processor_Dataframe_Is_Not_Stream_Exception_Alert: Error encountered during stream processing: {error}
"""
