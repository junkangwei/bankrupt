package org.bankrupt.common.constant;

/**
 * 返回的枚举类
 */
public enum ResultEnums {
    SEND_OK("SEND_OK"),
    CREATE_OK("CREATE_OK"),
    UPDATE_OFFSET_OK("UPDATE_OFFSET_OK");

    private String res;

    ResultEnums(String res) {
        this.res = res;
    }

    public String getRes() {
        return res;
    }
}
