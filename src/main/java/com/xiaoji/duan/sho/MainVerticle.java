package com.xiaoji.duan.sho;

import java.util.HashSet;
import java.util.Set;

import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;

public class MainVerticle extends AbstractVerticle {

	private WebClient client = null;
	private AmqpBridge bridge = null;

	@Override
	public void start(Future<Void> startFuture) throws Exception {
		client = WebClient.create(vertx);

		bridge = AmqpBridge.create(vertx);

		bridge.endHandler(endHandler -> {
			connectStompServer();
		});
		connectStompServer();

		Router router = Router.router(vertx);

		Set<HttpMethod> allowedMethods = new HashSet<HttpMethod>();
		allowedMethods.add(HttpMethod.OPTIONS);
		allowedMethods.add(HttpMethod.GET);
		allowedMethods.add(HttpMethod.POST);
		allowedMethods.add(HttpMethod.PUT);
		allowedMethods.add(HttpMethod.DELETE);
		allowedMethods.add(HttpMethod.CONNECT);
		allowedMethods.add(HttpMethod.PATCH);
		allowedMethods.add(HttpMethod.HEAD);
		allowedMethods.add(HttpMethod.TRACE);

		router.route()
				.handler(CorsHandler.create("*").allowedMethods(allowedMethods).allowedHeader("*")
						.allowedHeader("Content-Type").allowedHeader("lt").allowedHeader("pi").allowedHeader("pv")
						.allowedHeader("di").allowedHeader("dt").allowedHeader("ai"));

		router.route("/sho/linkless").handler(BodyHandler.create());
		router.route("/sho/linkless").produces("application/json").handler(this::linkless);

		HttpServerOptions option = new HttpServerOptions();
		option.setCompressionSupported(true);

		vertx.createHttpServer(option).requestHandler(router::accept).listen(8080, http -> {
			if (http.succeeded()) {
				startFuture.complete();
				System.out.println("HTTP server started on http://localhost:8080");
			} else {
				startFuture.fail(http.cause());
			}
		});
	}

	private void linkless(RoutingContext ctx) {
		System.out.println("headers: " + ctx.request().headers());
		System.out.println("body: " + ctx.getBodyAsString());

		String req = ctx.getBodyAsString();
		
		if (req == null || req.isEmpty()) {
			JsonObject ret = new JsonObject();
			ret.put("rc", "-1");
			ret.put("rm", "请求参数不存在, 非法请求!");

			ctx.response().putHeader("Content-Type", "application/json;charset=UTF-8").end(ret.encode());
			return;
		}
		
		JsonObject data = ctx.getBodyAsJson();

		String type = data.getString("type", "");
		String src = data.getString("src", "");
		System.out.println("type " + type + " src " + src);

		Future<JsonObject> future = Future.future();

		future.setHandler(handler -> {
			if (handler.succeeded()) {
				JsonObject su = handler.result();
				System.out.println(su);
				ctx.response().putHeader("Content-Type", "application/json; charset=utf-8").end(su.encode());
			} else {
				handler.cause().printStackTrace();
				JsonObject su = new JsonObject();

				su.put("url", src);
				su.put("err", handler.cause().getMessage());

				ctx.response().putHeader("Content-Type", "application/json; charset=utf-8").end(su.encode());
			}
		});

		sho(future, type, src);
	}

	private void connectStompServer() {
		bridge.start(config().getString("stomp.server.host", "sa-amq"), config().getInteger("stomp.server.port", 5672),
				res -> {
					if (res.failed()) {
						res.cause().printStackTrace();
						connectStompServer();
					} else {
						subscribeTrigger(config().getString("amq.app.id", "sho"));
					}
				});

	}

	private void subscribeTrigger(String trigger) {
		MessageConsumer<JsonObject> consumer = bridge.createConsumer(trigger);
		System.out.println("Consumer " + trigger + " subscribed.");
		consumer.handler(vertxMsg -> this.process(trigger, vertxMsg));
	}

	private void sho(Future<JsonObject> future, String type, String src) {
		StringBuffer api = new StringBuffer(config().getString("suolink.api", "http://api.suolink.cn/api.php"));
		api.append("?");

		api.append("format=");
		api.append("json");
		api.append("&url=");
		api.append(src);
		api.append("&key=");
		api.append(config().getString("suolink.api.appid", "5c9c94628e676d0bce1d1b25"));
		api.append("@");
		api.append(config().getString("suolink.api.secure", "9cb3208e23ed6e078851aac6ff546a2a"));

		client.getAbs(api.toString()).send(handler -> {
			if (handler.succeeded()) {
				HttpResponse<Buffer> result = handler.result();

				JsonObject su = result.bodyAsJsonObject();

				future.complete(su);
			} else {
				future.fail(handler.cause());
			}
		});
	}

	private void process(String consumer, Message<JsonObject> received) {
		System.out.println("Consumer " + consumer + " received [" + received.body().encode() + "]");
		JsonObject data = received.body().getJsonObject("body");

		String type = data.getJsonObject("context").getString("type", "");
		String src = data.getJsonObject("context").getString("src", "");
		String next = data.getJsonObject("context").getString("next");

		Future<JsonObject> future = Future.future();

		future.setHandler(handler -> {
			if (handler.succeeded()) {
				JsonObject su = handler.result();

				JsonObject nextctx = new JsonObject().put("context", new JsonObject().put("sho", su));

				MessageProducer<JsonObject> producer = bridge.createProducer(next);
				producer.send(new JsonObject().put("body", nextctx));
				System.out
						.println("Consumer " + consumer + " send to [" + next + "] result [" + nextctx.encode() + "]");
			} else {
				JsonObject su = new JsonObject();

				su.put("url", src);
				su.put("err", handler.cause().getMessage());

				JsonObject nextctx = new JsonObject().put("context", new JsonObject().put("sho", su));

				MessageProducer<JsonObject> producer = bridge.createProducer(next);
				producer.send(new JsonObject().put("body", nextctx));
				System.out
						.println("Consumer " + consumer + " send to [" + next + "] result [" + nextctx.encode() + "]");
			}
		});

		sho(future, type, src);
	}
}
