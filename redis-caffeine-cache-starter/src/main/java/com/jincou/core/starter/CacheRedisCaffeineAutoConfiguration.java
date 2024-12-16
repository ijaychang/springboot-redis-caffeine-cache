package com.jincou.core.starter;



import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.jincou.core.cache.RedisCache;
import com.jincou.core.config.L2CacheProperties;
import com.jincou.core.spring.RedisCaffeineCacheManager;
import com.jincou.core.sync.CacheMessageListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.support.NullValue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;


import java.io.IOException;
import java.net.UnknownHostException;


/**
 *  TODO
 *
 * @author xub
 * @date 2022/3/16 下午3:13
 */
@Configuration
@AutoConfigureAfter(RedisAutoConfiguration.class)
@EnableConfigurationProperties(L2CacheProperties.class)
public class CacheRedisCaffeineAutoConfiguration {

	@Autowired
	private L2CacheProperties l2CacheProperties;

	@Bean
	@ConditionalOnClass(RedisCache.class)
	@Order(2)
	public RedisCaffeineCacheManager cacheManager(RedisCache redisCache) {
		return new RedisCaffeineCacheManager(l2CacheProperties.getConfig(),redisCache);
	}

	@Bean
	@ConditionalOnMissingBean(name = "stringKeyRedisTemplate")
	public RedisTemplate<Object, Object> stringKeyRedisTemplate(RedisConnectionFactory redisConnectionFactory) throws UnknownHostException {
		RedisTemplate<Object, Object> redisTemplate = new RedisTemplate<>();
		redisTemplate.setConnectionFactory(redisConnectionFactory);

		// 用Jackson2JsonRedisSerializer来序列化和反序列化redis的value值
		Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<>(Object.class);
		ObjectMapper objectMapper = new ObjectMapper();
		// 指定要序列化的域(field,get,set)，访问修饰符(public,private,protected)
		objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
		objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		// Validator验证类用于验证是否能够被反序列化,DefaultTyping指定序列化输入的类型，类必须是非final修饰的，final修饰的类，比如String,Integer等会跑出异常
		objectMapper.activateDefaultTyping(LaissezFaireSubTypeValidator.instance, ObjectMapper.DefaultTyping.NON_FINAL);

		// 增加对NullValue的处理
		SimpleModule nullValueSimpleModule = new SimpleModule();
		nullValueSimpleModule.addSerializer(NullValue.class, NullValueSerializer.INSTANCE);
		nullValueSimpleModule.addDeserializer(NullValue.class, NullValueDeserializer.INSTANCE);
		objectMapper.registerModule(nullValueSimpleModule);

		jackson2JsonRedisSerializer.setObjectMapper(objectMapper);

		StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();

		// key采用String的序列化方式
		redisTemplate.setKeySerializer(stringRedisSerializer);
		// hash的key也采用String的序列化方式
		redisTemplate.setHashKeySerializer(stringRedisSerializer);
		// value序列化方式采用jackson
		redisTemplate.setValueSerializer(jackson2JsonRedisSerializer);
		// hash的value序列化方式采用jackson
		redisTemplate.setHashValueSerializer(jackson2JsonRedisSerializer);

		redisTemplate.afterPropertiesSet();
		return redisTemplate;
	}

	@Bean
	@ConditionalOnClass(RedisCache.class)
	@Order(3)
	public RedisMessageListenerContainer redisMessageListenerContainer(RedisCache redisCache,
																	   RedisCaffeineCacheManager cacheManager) {
		RedisMessageListenerContainer redisMessageListenerContainer = new RedisMessageListenerContainer();
		redisMessageListenerContainer.setConnectionFactory(redisCache.getRedisTemplate().getConnectionFactory());
		CacheMessageListener cacheMessageListener = new CacheMessageListener(redisCache, cacheManager);
		redisMessageListenerContainer.addMessageListener(cacheMessageListener, new ChannelTopic(l2CacheProperties.getConfig().getRedis().getTopic()));
		return redisMessageListenerContainer;
	}

	@Bean
	@ConditionalOnBean(RedisTemplate.class)
	@Order(1)
	public RedisCache redisCache(RedisTemplate<Object, Object> stringKeyRedisTemplate) {
		RedisCache redisCache = new RedisCache();
		redisCache.setRedisTemplate(stringKeyRedisTemplate);
		return redisCache;
	}

	public static class NullValueDeserializer extends StdDeserializer<NullValue> {

		public static final NullValueDeserializer INSTANCE = new NullValueDeserializer();

		protected NullValueDeserializer() {
			super(NullValue.class);
		}

		@Override
		public NullValue deserialize(JsonParser p, DeserializationContext ctx) {
			return (NullValue) NullValue.INSTANCE;
		}

	}

	public static class NullValueSerializer extends StdSerializer<NullValue> {

		public static final NullValueSerializer INSTANCE = new NullValueSerializer();

		protected NullValueSerializer() {
			super(NullValue.class);
		}


		@Override
		public void serialize(NullValue value, JsonGenerator gen, SerializerProvider provider) throws IOException {
			gen.writeNull();
		}
	}
}
