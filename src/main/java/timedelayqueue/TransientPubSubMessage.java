package timedelayqueue;

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

// TODO: write a description for this class
// TODO: complete all methods, irrespective of whether there is an explicit TODO or not
// TODO: write clear specs
// TODO: State the rep invariant and abstraction function
// TODO: what is the thread safety argument?
public class TransientPubSubMessage extends PubSubMessage {
    private final int     lifetime;
    private final boolean isTransient = true;

    // TODO: add other constructors to match the supertype

    public TransientPubSubMessage(UUID id, Timestamp timestamp, UUID sender, UUID receiver,
                                  String content, MessageType type, int lifetime) {
        super(id, timestamp, sender, receiver, content, type);
        this.lifetime = lifetime;
    }

    public TransientPubSubMessage(UUID id, Timestamp timestamp, UUID sender, List<UUID> receiver,
                                  String content, MessageType type, int lifetime) {
        super(id, timestamp, sender, receiver, content, type);
        this.lifetime = lifetime;
    }

    public TransientPubSubMessage(UUID sender, UUID receiver, String content, int lifetime) {
        super(sender, receiver, content);
        this.lifetime = lifetime;
    }

    public TransientPubSubMessage(UUID sender, List<UUID> receiver, String content, int lifetime) {
        super(sender, receiver, content);
        this.lifetime = lifetime;
    }



    public int getLifetime() {
        return lifetime;
    }

    @Override
    public boolean isTransient() {
        return isTransient;
    }
}
