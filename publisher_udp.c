/*
 * publisher_udp.c  -  Laboratorio 3 (ISIS-2311L, Uniandes 2026-20)  -  ESQUELETO
 * Grupo: ____    Integrantes: ____________ (codigo) / ____________ / ____________
 *
 * Rol (guia oficial, sec. 4.1 y 4.2)
 *   Periodista de UN partido: envia n_mensajes datagramas al broker, uno cada
 *   intervalo_ms milisegundos. Sin connect: cada sendto lleva el destino (p.207).
 *
 * Mensaje (un datagrama por evento):
 *   "PUB <partido> <seq> <texto>\n"      seq = 1, 2, 3, ...
 *   El seq es la UNICA forma que tiene el suscriptor de saber que algo se perdio
 *   o llego desordenado: UDP no numera nada por usted.
 *
 * YA HECHO    socket (p.190), direccion del broker, pausa, hora de envio, cierre.
 * POR HACER   TODO 1 y TODO 2.
 *
 * Compilar:  make udp
 * Ejecutar:  ./publisher_udp 127.0.0.1 5001 AvsB 10 200
 *            (intervalo_ms = 0 envia en rafaga: util para provocar perdidas, Ej.6 y Ej.8)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#define DATAGRAMA_MAX 512
#define PARTIDO_MAX   32

static const char *EVENTOS[] = {
    "Inicio del partido",
    "Tarjeta amarilla al numero 10 de Equipo B",
    "Gol de Equipo A al minuto 32",
    "Marcador 1-0",
    "Cambio: jugador 10 entra por jugador 20",
    "Gol de Equipo B al minuto 58",
    "Marcador 1-1",
    "Tarjeta roja al numero 4 de Equipo A",
    "Gol de Equipo B al minuto 81",
    "Marcador 1-2",
    "Final del partido",
};
#define N_EVENTOS (sizeof EVENTOS / sizeof EVENTOS[0])

/* leer_entero() - convierte un argumento de consola a entero y valida que este
 * entre min y max. Si el texto no es un numero o se sale del rango, imprime el
 * error y termina el programa: es preferible no arrancar a enviar basura. */
static long leer_entero(const char *txt, long min, long max, const char *que)
{
    char *fin;
    long v = strtol(txt, &fin, 10);
    if (*fin != '\0' || v < min || v > max) {
        fprintf(stderr, "%s invalido: %s\n", que, txt);
        exit(1);
    }
    return v;
}

/* dormir_ms() - pausa entre eventos. Reintenta mientras nanosleep sea
 * interrumpido por una senal (EINTR), para que la pausa dure lo pedido. */
static void dormir_ms(long ms)
{
    struct timespec t = { .tv_sec = ms / 1000, .tv_nsec = (ms % 1000) * 1000000L };
    while (nanosleep(&t, &t) == -1 && errno == EINTR) { }
}

/* hora_actual() - marca de tiempo HH:MM:SS.mmm del envio. Sirve para cruzar la
 * salida del publicador con las capturas de Wireshark (punto 4 del enunciado). */
static void hora_actual(char *out, size_t n)
{
    struct timespec ts;
    struct tm tm;
    clock_gettime(CLOCK_REALTIME, &ts);
    localtime_r(&ts.tv_sec, &tm);
    size_t k = strftime(out, n, "%H:%M:%S", &tm);
    snprintf(out + k, n - k, ".%03ld", ts.tv_nsec / 1000000L);
}

int main(int argc, char *argv[])
{
    if (argc != 6) {
        fprintf(stderr, "Uso: %s <ip_broker> <puerto> <partido> <n_mensajes> <intervalo_ms>\n", argv[0]);
        return 1;
    }
    int  puerto     = (int)leer_entero(argv[2], 1, 65535, "Puerto");
    const char *partido = argv[3];
    long n_mensajes = leer_entero(argv[4], 1, 1000000, "n_mensajes");
    long intervalo  = leer_entero(argv[5], 0, 60000, "intervalo_ms");
    if (strlen(partido) >= PARTIDO_MAX || strchr(partido, ' ') != NULL) {
        fprintf(stderr, "El partido debe tener menos de %d caracteres y ningun espacio\n", PARTIDO_MAX);
        return 1;
    }
    setvbuf(stdout, NULL, _IOLBF, 0);

    /* L93   socket(): descriptor UDP. A diferencia de TCP no hay connect ni estado
 * de conexion; el socket queda listo para enviar de inmediato. */
    int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s == -1) { perror("socket"); return 1; }

    struct sockaddr_in broker;
    memset(&broker, 0, sizeof broker);
    broker.sin_family = AF_INET;
    broker.sin_port   = htons((uint16_t)puerto);
    if (inet_pton(AF_INET, argv[1], &broker.sin_addr) != 1) {
        fprintf(stderr, "IP invalida: %s\n", argv[1]);
        close(s);
        return 1;
    }
    printf("[pub %s] enviando a %s:%d (sin conexion)\n", partido, argv[1], puerto);

    for (long i = 0; i < n_mensajes; i++) {
        const char *texto = EVENTOS[(size_t)i % N_EVENTOS];
        char dgm[DATAGRAMA_MAX] = "";
        int  lg = 0;
        char hora[32];
        hora_actual(hora, sizeof hora);

    
        /* L115  snprintf(): arma el datagrama con el numero de secuencia. Se verifica
 * que no haya truncamiento (lg >= sizeof dgm); si no cabe, se salta el evento
 * en vez de enviar un mensaje cortado que el suscriptor no podria parsear. */
    lg = snprintf(dgm, sizeof dgm, "PUB %s %ld %s\n", partido, i + 1, texto);
            if (lg<=0 || lg >= (int)sizeof dgm){
            fprintf(stderr, "[pub %s] seq=%ld: mensaje demasiado largo\n", partido, i+1);
            continue;
        }
    
        /* L121  sendto(): un datagrama por evento, con la direccion del broker en cada
 * llamada. El fallo se reporta y el bucle continua: en UDP un envio perdido no
 * rompe nada, que es justo lo que el laboratorio quiere medir. */
    if (sendto(s, dgm, (size_t)lg, 0, (struct sockaddr *)&broker, sizeof broker) < 0) {
        perror("sendto");
    } else {
        printf("[pub %s] %s enviado seq=%ld: %s\n", partido, hora, i + 1, texto);
    }

        if (intervalo > 0) dormir_ms(intervalo);
    }


    /* L130  close(): en UDP no envia nada al otro lado; el broker no se entera de
 * que el publicador termino. */
    close(s);     /* p.206: en UDP close no envia nada al otro lado */
    printf("[pub %s] fin: %ld mensajes procesados\n", partido, n_mensajes);
    return 0;
}
