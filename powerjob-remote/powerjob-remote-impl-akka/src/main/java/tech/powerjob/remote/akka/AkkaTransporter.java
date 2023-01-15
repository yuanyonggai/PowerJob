package tech.powerjob.remote.akka;

import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import com.google.common.collect.Maps;
import tech.powerjob.common.PowerSerializable;
import tech.powerjob.common.RemoteConstant;
import tech.powerjob.common.request.ServerScheduleJobReq;
import tech.powerjob.common.utils.CommonUtils;
import tech.powerjob.remote.framework.base.RemotingException;
import tech.powerjob.remote.framework.base.ServerType;
import tech.powerjob.remote.framework.base.URL;
import tech.powerjob.remote.framework.transporter.Protocol;
import tech.powerjob.remote.framework.transporter.Transporter;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

/**
 * AkkaTransporter
 *
 * @author tjq
 * @since 2022/12/31
 */
public class AkkaTransporter implements Transporter {

    private final ActorSystem actorSystem;

    private final String targetActorSystemName;

    /**
     * akka://<actor system>@<hostname>:<port>/<actor path>
     */
    private static final String AKKA_NODE_PATH = "akka://%s@%s/user/%s";

    public AkkaTransporter(ServerType serverType, ActorSystem actorSystem) {
        this.actorSystem = actorSystem;
        this.targetActorSystemName = AkkaConstant.fetchActorSystemName(serverType, false);
    }

    @Override
    public Protocol getProtocol() {
        return new AkkaProtocol();
    }

    @Override
    public void tell(URL url, PowerSerializable request) {
        ActorSelection actorSelection = fetchActorSelection(url);
        actorSelection.tell(request, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> CompletionStage<T> ask(URL url, PowerSerializable request, Class<T> clz) throws RemotingException {
        ActorSelection actorSelection = fetchActorSelection(url);
        return (CompletionStage<T>) Patterns.ask(actorSelection, request, Duration.ofMillis(RemoteConstant.DEFAULT_TIMEOUT_MS));
    }

    private ActorSelection fetchActorSelection(URL url) {

        String targetActorName = AkkaMappingService.parseActorName(url.getLocation().getRootPath()).getActorName();

        CommonUtils.requireNonNull(targetActorName, "can't find actor by URL: " + url.getLocation());

        String address = url.getAddress().toFullAddress();

        return actorSystem.actorSelection(String.format(AKKA_NODE_PATH, targetActorSystemName, address, targetActorName));
    }
}