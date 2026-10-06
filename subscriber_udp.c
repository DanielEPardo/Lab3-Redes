
/*
 * subscriber_udp.c  -  Laboratorio 3 (ISIS-2311L, Uniandes 2026-20)  -  ESQUELETO
 * Grupo: ____    Integrantes: ____________ (codigo) / ____________ / ____________
 *
 * Rol (guia oficial, sec. 4.2)
 *   Hincha que sigue uno o varios partidos por UDP. Envia "SUB <partido>\n" al
 *   broker y muestra cada datagrama PUB que le llega. Como UDP no garantiza
 *   nada (p.207), usa el numero de secuencia para CONTAR perdidas, desorden y
 *   duplicados. Termina tras <espera_s> segundos sin recibir nada.
 *
 * YA HECHO    socket (p.190), timeout de inactividad (SO_RCVTIMEO), bucle de
 *             recvfrom (p.208), tabla de estados, RESUMEN.
 * POR HACER   TODO 1 (suscribirse), TODO 2 (parsear), TODO 3 (huecos, desorden, duplicados).
 *
 * Compilar:  make udp
 * Ejecutar:  ./subscriber_udp 127.0.0.1 5001 3 AvsB CvsD      (3 = segundos de inactividad)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>

#define DATAGRAMA_MAX 512
#define PARTIDO_MAX   32
#define MAX_PARTIDOS  8
#define SEQ_MAX       65535     /* seq mayores se ignoran (tamano del arreglo "visto") */

typedef struct {
    char          partido[PARTIDO_MAX];
    unsigned      esperado;           /* proximo seq que deberia llegar (empieza en 1) */
    unsigned      recibidos;          /* datagramas distintos recibidos */
    unsigned      perdidos;           /* huecos que siguen abiertos (posibles perdidas) */
    unsigned      desordenados;       /* llegaron despues de uno mayor */
    unsigned      duplicados;         /* el mismo seq otra vez */
    unsigned      ultimo_seq;         /* el mayor seq visto */
    unsigned char visto[SEQ_MAX + 1]; /* visto[seq] = 1 si ya llego ese seq */
} EstadoPartido;

EstadoPartido estados[MAX_PARTIDOS];
int n_estados = 0;

static volatile sig_atomic_t seguir = 1;
static void al_recibir_senal(int s) { (void)s; seguir = 0; }

EstadoPartido *buscar_estado(const char *partido);                         /* hecho  */
void           imprimir_resumen(void);                                     /* hecho  */
void           suscribirse(int s, const struct sockaddr_in *broker);       /* TODO 1 */
void           procesar_datagrama(char *dgm);                              /* TODO 2 */
void           registrar_seq(EstadoPartido *e, unsigned seq);              /* TODO 3 */

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

void hora_actual(char *out, size_t n)
{
    struct timespec ts;
    struct tm tm;
    clock_gettime(CLOCK_REALTIME, &ts);
    localtime_r(&ts.tv_sec, &tm);
    size_t k = strftime(out, n, "%H:%M:%S", &tm);
    snprintf(out + k, n - k, ".%03ld", ts.tv_nsec / 1000000L);
}

static void dormir_ms(long ms)
{
    struct timespec t = { .tv_sec = ms / 1000, .tv_nsec = (ms % 1000) * 1000000L };
    while (nanosleep(&t, &t) == -1 && errno == EINTR) { }
}

EstadoPartido *buscar_estado(const char *partido)
{
    for (int i = 0; i < n_estados; i++)
        if (strcmp(estados[i].partido, partido) == 0) return &estados[i];
    return NULL;
}

void imprimir_resumen(void)
{
    for (int i = 0; i < n_estados; i++)
        printf("RESUMEN partido=%s recibidos=%u perdidos=%u desordenados=%u duplicados=%u ultimo_seq=%u\n",
               estados[i].partido, estados[i].recibidos, estados[i].perdidos,
               estados[i].desordenados, estados[i].duplicados, estados[i].ultimo_seq);
    printf("NOTA: si se perdieron los ULTIMOS mensajes, no hay hueco que los delate; "
           "compare ultimo_seq con lo que dice el log del publicador.\n");
}

/* Envia "SUB <partido>\n" al broker por cada partido. */
void suscribirse(int s, const struct sockaddr_in *broker)
{

    for (int i=0; i<n_estados; i++){ // cuantos partidos hay
        char linea[64];
        int lg = snprintf(linea, sizeof linea, "SUB %s\n", estados[i].partido);
        if (lg<=0 || lg >= (int)sizeof linea){   //* (3) la misma revisión del publisher */
            fprintf(stderr, "[sub] partido demasiado largo\n");
            continue;
        }
        for (int k = 0; k < 3; k++) {                       /* 3 envíos: el SUB también se puede perder */
            if (sendto(s, linea, (size_t)lg, 0, (const struct sockaddr *)broker, sizeof *broker) == -1)
                perror("sendto");
            dormir_ms(50);
        }
        printf("[sub] SUB %s enviado 3 veces\n", estados[i].partido);    
    }
}

/* Procesa un datagrama "PUB <partido> <seq> <texto>\n" (terminado en '\0'). */
void procesar_datagrama(char *dgm)
{
    char partido[PARTIDO_MAX];
    unsigned seq;
    int pos = 0;

    if (sscanf(dgm, "PUB %31s %u %n", partido, &seq, &pos) < 2) {
        return; // no es datagrama valido
    }

    EstadoPartido *e = buscar_estado(partido);
    if (e == NULL) {
        return; // si no se esta suscrito a ese partido
    }

    char *texto = dgm + pos;
    size_t len = strlen(texto);
    if (len > 0 && texto[len - 1] == '\n') {
        texto[len - 1] = '\0';
    }

    // imprimir hora de llegada
    char h[32];
    hora_actual(h, sizeof h);
    printf("[sub] %s partido=%s seq=%u %s\n", h, partido, seq, texto);

    registrar_seq(e, seq);
}

/* Actualiza los contadores de un partido con el seq recibido. */
void registrar_seq(EstadoPartido *e, unsigned seq)
{
    if (e == NULL || seq == 0 || seq > SEQ_MAX) {
        return; // secuencia invalida
    }

    if (e->visto[seq]) {
        e->duplicados++;
        return; // seq duplicado al ya haberse recibido antes
    }

    // se marca como visto y se incrementa el contador de recibidos
    e->visto[seq] = 1;
    e->recibidos++;

    if (seq == e->esperado) {
        // llego el seq que se esperaba que llegara
        e->esperado++;
    } else if (seq > e->esperado) {
        // hubo un salto al ser la seq mayor, posibles perdidas
        e->perdidos += (seq - e->esperado);
        printf("[sub] HUECO en %s: faltan %u..%u\n", e->partido, e->esperado, seq - 1);
        e->esperado = seq + 1;
    } else {
        // llego un paquete viejo
        e->desordenados++;
        if (e->perdidos > 0) {
            e->perdidos--;
        }
        printf("[sub] DESORDEN en %s: llego seq=%u despues de %u\n", e->partido, seq, e->ultimo_seq);
    }

    // se actualiza el ultimo seq visto
    if (seq > e->ultimo_seq) {
        e->ultimo_seq = seq;
    }
}

int main(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr, "Uso: %s <ip_broker> <puerto> <espera_s> <partido> [partido2 ...]\n", argv[0]);
        return 1;
    }
    int  puerto = (int)leer_entero(argv[2], 1, 65535, "Puerto");
    long espera = leer_entero(argv[3], 1, 3600, "espera_s");
    setvbuf(stdout, NULL, _IOLBF, 0);

    for (int i = 4; i < argc && n_estados < MAX_PARTIDOS; i++) {
        if (strlen(argv[i]) >= PARTIDO_MAX) { fprintf(stderr, "Partido muy largo: %s\n", argv[i]); return 1; }
        memset(&estados[n_estados], 0, sizeof estados[n_estados]);
        snprintf(estados[n_estados].partido, PARTIDO_MAX, "%s", argv[i]);
        estados[n_estados].esperado = 1;
        n_estados++;
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = al_recibir_senal;          /* sin SA_RESTART: recvfrom() retorna EINTR */
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* p.190: socket UDP. Sin bind: el SO asigna puerto efimero en el primer sendto. */
    int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s == -1) { perror("socket"); return 1; }

    /* Fin por inactividad: recvfrom devuelve -1 con EAGAIN tras "espera" segundos. */
    struct timeval tv = { .tv_sec = espera, .tv_usec = 0 };
    if (setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv) == -1)
        perror("setsockopt(SO_RCVTIMEO)");

    struct sockaddr_in broker;
    memset(&broker, 0, sizeof broker);
    broker.sin_family = AF_INET;
    broker.sin_port   = htons((uint16_t)puerto);
    if (inet_pton(AF_INET, argv[1], &broker.sin_addr) != 1) {
        fprintf(stderr, "IP invalida: %s\n", argv[1]);
        close(s);
        return 1;
    }

    suscribirse(s, &broker);
    printf("[sub] esperando datagramas de %s:%d (fin tras %ld s sin datos)\n", argv[1], puerto, espera);

    char dgm[DATAGRAMA_MAX + 1];
    while (seguir) {
        struct sockaddr_in origen;
        socklen_t lg = sizeof origen;
        ssize_t n = recvfrom(s, dgm, DATAGRAMA_MAX, 0, (struct sockaddr *)&origen, &lg);   /* p.208 */
        if (n == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                printf("[sub] %ld s sin datos: fin\n", espera);
                break;
            }
            if (errno == EINTR) continue;
            perror("recvfrom");
            break;
        }
        dgm[n] = '\0';
        procesar_datagrama(dgm);
    }

    imprimir_resumen();
    close(s);
    return 0;
}
