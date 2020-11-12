package site.assad.datastream;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * @author yulinying
 * @since 2020/11/12
 */
public class MyMessage {
    
    private String content;
    private long happenTime;
    public MyMessage() {
    }
    
    public String getContent() {
        return content;
    }
    
    public void setContent(String content) {
        this.content = content;
    }
    
    public long getHappenTime() {
        return happenTime;
    }
    
    public void setHappenTime(long happenTime) {
        this.happenTime = happenTime;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MyMessage myMessage = (MyMessage) o;
        return new EqualsBuilder()
                .append(happenTime, myMessage.happenTime)
                .append(content, myMessage.content)
                .isEquals();
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(content)
                .append(happenTime)
                .toHashCode();
    }
}
