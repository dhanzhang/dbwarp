package com.jpdbase.dbwarp;

import com.jpdbase.dbwarp.EventHandler.DicomAttrHandle;
import com.jpdbase.dbwarp.EventHandler.DicomJsonHandle;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class EventHandlerFactory {

    public static final String PARTICAL_KEY = "hiscode";

    private final Map<String, IEventHandler> handlerMap;

    public static final String[] SUPPORTED_TALBES = {DicomJsonHandle.TABLENAME, DicomAttrHandle.TABLENAME};

    private EventHandlerFactory() {
        handlerMap = new HashMap<>(2);
        handlerMap.put(DicomAttrHandle.TABLENAME, new DicomAttrHandle());
        handlerMap.put(DicomJsonHandle.TABLENAME, new DicomJsonHandle());
    }


    private static class FactoryHolder {

        private static final EventHandlerFactory factory = new EventHandlerFactory();
    }

    public static EventHandlerFactory getFactoryInstance() {
        return FactoryHolder.factory;
    }

    public IEventHandler BuilderHandler(String tableName) {
        if (handlerMap.containsKey(tableName)) {
            return handlerMap.get(tableName);
        }
        return null;

    }

}
