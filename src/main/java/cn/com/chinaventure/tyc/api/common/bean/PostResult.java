package cn.com.chinaventure.tyc.api.common.bean;

/**
 * 用来处理post返回的json
 * Created by YZP on 2018/4/26.
 */
public class PostResult {

    public  String reason;
    public  String error_code;
    public  String result;

    public PostResult(String reason, String error_code, String result) {
        this.reason = reason;
        this.error_code = error_code;
        this.result = result;
    }


    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getError_code() {
        return error_code;
    }

    public void setError_code(String error_code) {
        this.error_code = error_code;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "PostResult{" +
                "reason='" + reason + '\'' +
                ", error_code='" + error_code + '\'' +
                ", result='" + result + '\'' +
                '}';
    }
}





